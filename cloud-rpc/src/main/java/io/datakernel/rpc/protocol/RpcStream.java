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
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

public final class RpcStream {
	private static final CloseException RPC_CLOSE_EXCEPTION = new CloseException(RpcStream.class, "RPC Channel Closed");
	private final ChannelDeserializer<RpcMessage> deserializer;
	private final ChannelSerializer<RpcMessage> serializer;

	public interface Listener extends StreamDataAcceptor<RpcMessage> {
		void onReceiverEndOfStream();

		void onReceiverError(@NotNull Throwable e);

		void onSenderReady(@NotNull StreamDataAcceptor<RpcMessage> acceptor);

		void onSenderSuspended();

		void onSenderError(@NotNull Throwable e);
	}

	@SuppressWarnings("FieldCanBeLocal")
	private final boolean server;
	private final AsyncTcpSocket socket;
	private Listener listener;

	public RpcStream(AsyncTcpSocket socket,
			BinarySerializer<RpcMessage> messageSerializer,
			MemSize initialBufferSize, MemSize maxMessageSize,
			Duration autoFlushInterval, boolean compression, boolean server) {
		this.server = server;
		this.socket = socket;

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

		this.deserializer = deserializer;
		this.serializer = serializer;
	}

	public void setListener(Listener listener) {
		this.listener = listener;
		deserializer.getEndOfStream()
				.whenResult(listener::onReceiverEndOfStream)
				.whenException(listener::onReceiverError);
		serializer.getAcknowledgement()
				.whenException(listener::onSenderError);
		serializer.consume(
				acceptor -> {
					if (acceptor != null) {
						deserializer.supply(listener);
						listener.onSenderReady(acceptor);
					} else {
						if (server) {
							deserializer.supply(null);
						}
						listener.onSenderSuspended();
					}
				}
		);
	}

	public void sendEndOfStream() {
		if (!serializer.getAcknowledgement().isComplete()) {
			serializer.endOfStream();
		}
	}

	public void close() {
		socket.close(RPC_CLOSE_EXCEPTION);
	}
}
