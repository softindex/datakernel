/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.rpc.protocol;

import io.datakernel.common.MemSize;
import io.datakernel.common.exception.CloseException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelLZ4Compressor;
import io.datakernel.csp.process.ChannelLZ4Decompressor;
import io.datakernel.datastream.AbstractStreamConsumer;
import io.datakernel.datastream.AbstractStreamSupplier;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class RpcStream {
	private static final CloseException RPC_CLOSE_EXCEPTION = new CloseException(RpcStream.class, "RPC Channel Closed");

	public interface Listener extends StreamDataAcceptor<RpcMessage> {
		void onReceiverEndOfStream();

		void onReceiverError(@NotNull Throwable e);

		void onSenderError(@NotNull Throwable e);

		void onSenderReady(@NotNull StreamDataAcceptor<RpcMessage> acceptor);

		void onSenderSuspended();
	}

	private final boolean server;
	private final AsyncTcpSocket socket;
	private Listener listener;
	private final AbstractStreamSupplier<RpcMessage> sender;
	private final AbstractStreamConsumer<RpcMessage> receiver;

	public RpcStream(AsyncTcpSocket socket,
			BinarySerializer<RpcMessage> messageSerializer,
			MemSize initialBufferSize, MemSize maxMessageSize,
			Duration autoFlushInterval, boolean compression, boolean server) {
		this.server = server;
		this.socket = socket;
		if (this.server) {
			sender = new AbstractStreamSupplier<RpcMessage>() {
				@Override
				protected void onProduce(StreamDataAcceptor<RpcMessage> dataAcceptor) {
					receiver.getSupplier().resume(listener);
					listener.onSenderReady(dataAcceptor);
				}

				@Override
				protected void onSuspended() {
					receiver.getSupplier().suspend();
					listener.onSenderSuspended();
				}

				@Override
				protected void onError(Throwable e) {
					if (e != RPC_CLOSE_EXCEPTION) listener.onSenderError(e);
				}
			};
		} else {
			sender = new AbstractStreamSupplier<RpcMessage>() {
				@Override
				protected void onProduce(StreamDataAcceptor<RpcMessage> dataAcceptor) {
					listener.onSenderReady(dataAcceptor);
				}

				@Override
				protected void onSuspended() {
					listener.onSenderSuspended();
				}

				@Override
				protected void onError(Throwable e) {
					if (e != RPC_CLOSE_EXCEPTION) listener.onSenderError(e);
				}
			};
		}

		receiver = new AbstractStreamConsumer<RpcMessage>() {
			@Override
			protected void onStarted() {
				getSupplier().resume(listener);
			}

			@Override
			protected Promise<Void> onEndOfStream() {
				listener.onReceiverEndOfStream();
				return Promise.complete();
			}

			@Override
			protected void onError(Throwable e) {
				if (e != RPC_CLOSE_EXCEPTION) listener.onReceiverError(e);
			}
		};

		ChannelSerializer<RpcMessage> serializer = ChannelSerializer.create(messageSerializer)
				.withInitialBufferSize(initialBufferSize)
				.withMaxMessageSize(maxMessageSize)
				.withAutoFlushInterval(autoFlushInterval)
				.withSkipSerializationErrors();
		ChannelDeserializer<RpcMessage> deserializer = ChannelDeserializer.create(messageSerializer);

		if (compression) {
			ChannelLZ4Decompressor decompressor = ChannelLZ4Decompressor.create();
			ChannelLZ4Compressor compressor = ChannelLZ4Compressor.createFastCompressor();

			ChannelSupplier.ofSocket(socket).bindTo(decompressor.getInput());
			decompressor.getOutput().bindTo(deserializer.getInput());

			serializer.getOutput().bindTo(compressor.getInput());
			compressor.getOutput().set(ChannelConsumer.ofSocket(socket));
		} else {
			ChannelSupplier.ofSocket(socket).bindTo(deserializer.getInput());
			serializer.getOutput().set(ChannelConsumer.ofSocket(socket));
		}

		deserializer.streamTo(receiver);
		sender.streamTo(serializer);
	}

	public void setListener(Listener listener) {
		this.listener = listener;
	}

	public void sendEndOfStream() {
		sender.sendEndOfStream();
	}

	public void close() {
		getCurrentEventloop().post(() -> socket.close(RPC_CLOSE_EXCEPTION));
	}
}
