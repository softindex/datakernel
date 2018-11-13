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

import io.datakernel.async.Promise;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.serial.processor.SerialLZ4Compressor;
import io.datakernel.serial.processor.SerialLZ4Decompressor;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.AbstractStreamSupplier;
import io.datakernel.stream.StreamDataAcceptor;
import io.datakernel.util.MemSize;

import java.time.Duration;

@SuppressWarnings("unchecked")
public final class RpcStream {
	public interface Listener extends StreamDataAcceptor<RpcMessage> {
		void onClosedWithError(Throwable exception);

		void onReadEndOfStream();
	}

	private Listener listener;
	private final AbstractStreamSupplier<RpcMessage> sender;
	private final AbstractStreamConsumer<RpcMessage> receiver;

	private boolean ready;
	private StreamDataAcceptor<RpcMessage> downstreamDataAcceptor;

	public RpcStream(AsyncTcpSocket socket,
			BufferSerializer<RpcMessage> messageSerializer,
			MemSize initialBufferSize, MemSize maxMessageSize,
			Duration autoFlushInterval, boolean compression, boolean server) {

		if (server) {
			sender = new AbstractStreamSupplier<RpcMessage>() {
				@Override
				protected void onProduce(StreamDataAcceptor<RpcMessage> dataAcceptor) {
					downstreamDataAcceptor = dataAcceptor;
					receiver.getSupplier().resume(listener);
					ready = true;
				}

				@Override
				protected void onSuspended() {
					receiver.getSupplier().suspend();
					ready = false;
				}

				@Override
				protected void onError(Throwable t) {
					listener.onClosedWithError(t);
					ready = false;
				}
			};
		} else {
			sender = new AbstractStreamSupplier<RpcMessage>() {
				@Override
				protected void onProduce(StreamDataAcceptor<RpcMessage> dataAcceptor) {
					downstreamDataAcceptor = dataAcceptor;
					ready = true;
				}

				@Override
				protected void onSuspended() {
					ready = false;
				}

				@Override
				protected void onError(Throwable t) {
					listener.onClosedWithError(t);
					ready = false;
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
				listener.onReadEndOfStream();
				return Promise.complete();
			}

			@Override
			protected void onError(Throwable t) {
			}
		};

		SerialBinarySerializer<RpcMessage> serializer = SerialBinarySerializer.create(messageSerializer)
				.withInitialBufferSize(initialBufferSize)
				.withMaxMessageSize(maxMessageSize)
				.withAutoFlushInterval(autoFlushInterval)
				.withSkipSerializationErrors();
		SerialBinaryDeserializer<RpcMessage> deserializer = SerialBinaryDeserializer.create(messageSerializer);

		if (compression) {
			SerialLZ4Decompressor decompressor = SerialLZ4Decompressor.create();
			SerialLZ4Compressor compressor = SerialLZ4Compressor.createFastCompressor();

			SerialSupplier.ofSocket(socket).bindTo(decompressor.getInput());
			decompressor.getOutput().bindTo(deserializer.getInput());

			serializer.getOutput().bindTo(compressor.getInput());
			compressor.getOutput().set(SerialConsumer.ofSocket(socket));
		} else {
			SerialSupplier.ofSocket(socket).bindTo(deserializer.getInput());
			serializer.getOutput().set(SerialConsumer.ofSocket(socket));
		}

		deserializer.streamTo(receiver);
		sender.streamTo(serializer);
	}

	public void setListener(Listener listener) {
		this.listener = listener;
	}

	public void sendMessage(RpcMessage message) {
		sendRpcMessage(message);
	}

	public void sendCloseMessage() {
		sendRpcMessage(RpcMessage.of(-1, RpcControlMessage.CLOSE));
	}

	private void sendRpcMessage(RpcMessage message) {
		if (ready) {
			downstreamDataAcceptor.accept(message);
		}
	}

	public boolean isOverloaded() {
		return !ready;
	}

	public void sendEndOfStream() {
		sender.sendEndOfStream();
	}
}
