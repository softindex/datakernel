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

import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamStatus;
import io.datakernel.stream.net.SocketStreamingConnection;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;

@SuppressWarnings("unchecked")
public final class RpcStream {
	public interface Listener extends StreamDataReceiver<RpcMessage> {
		void onClosedWithError(Throwable exception);

		void onReadEndOfStream();
	}

	private final Eventloop eventloop;
	private Listener listener;
	private final AbstractStreamProducer<RpcMessage> sender;
	private final AbstractStreamConsumer<RpcMessage> receiver;
	private final StreamBinarySerializer<RpcMessage> serializer;
	private final StreamBinaryDeserializer<RpcMessage> deserializer;
	private final boolean compression;
	private final StreamLZ4Compressor compressor;
	private final StreamLZ4Decompressor decompressor;
	private final SocketStreamingConnection connection;

	private StreamStatus producerStatus;
	private StreamDataReceiver<RpcMessage> downstreamDataReceiver;

	public RpcStream(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                 BufferSerializer<RpcMessage> messageSerializer,
	                 int defaultPacketSize, int maxPacketSize,
	                 int autoFlushIntervalMillis, boolean compression, boolean server) {
		this.eventloop = eventloop;
		this.compression = compression;

		connection = SocketStreamingConnection.create(eventloop, asyncTcpSocket);

		if (server) {
			sender = new AbstractStreamProducer<RpcMessage>(eventloop) {
				@Override
				protected void onProduce(StreamDataReceiver<RpcMessage> dataReceiver) {
					RpcStream.this.downstreamDataReceiver = dataReceiver;
					receiver.getProducer().produce(RpcStream.this.listener);
					producerStatus = getStatus();
				}

				@Override
				protected void onSuspended() {
					receiver.getProducer().suspend();
					producerStatus = getStatus();
				}

				@Override
				protected void onError(Exception e) {
					RpcStream.this.listener.onClosedWithError(e);
					producerStatus = getStatus();
				}
			};
		} else {
			sender = new AbstractStreamProducer<RpcMessage>(eventloop) {
				@Override
				protected void onProduce(StreamDataReceiver<RpcMessage> dataReceiver) {
					RpcStream.this.downstreamDataReceiver = dataReceiver;
					producerStatus = getStatus();
				}

				@Override
				protected void onSuspended() {
					producerStatus = getStatus();
				}

				@Override
				protected void onError(Exception e) {
					RpcStream.this.listener.onClosedWithError(e);
					producerStatus = getStatus();
				}
			};
		}

		receiver = new AbstractStreamConsumer<RpcMessage>(eventloop) {
			@Override
			protected void onStarted() {
				getProducer().produce(RpcStream.this.listener);
			}

			@Override
			protected void onEndOfStream() {
				RpcStream.this.listener.onReadEndOfStream();
			}

			@Override
			protected void onError(Exception e) {
			}
		};

		serializer = StreamBinarySerializer.create(eventloop, messageSerializer)
				.withDefaultBufferSize(defaultPacketSize)
				.withMaxMessageSize(maxPacketSize)
				.withAutoFlush(autoFlushIntervalMillis)
				.withSkipSerializationErrors()
//				.withInspector(serializerInspector)
		;
		deserializer = StreamBinaryDeserializer.create(eventloop, messageSerializer);

		if (compression) {
			compressor = StreamLZ4Compressor.fastCompressor(eventloop);
			decompressor = StreamLZ4Decompressor.create(eventloop);
			connection.getSocketReader().streamTo(decompressor.getInput());
			decompressor.getOutput().streamTo(deserializer.getInput());

			serializer.getOutput().streamTo(compressor.getInput());
			connection.getSocketWriter().streamFrom(compressor.getOutput());
		} else {
			compressor = null;
			decompressor = null;
			connection.getSocketReader().streamTo(deserializer.getInput());
			connection.getSocketWriter().streamFrom(serializer.getOutput());
		}
	}

	public void setListener(Listener listener) {
		this.listener = listener;

		producerStatus = sender.getStatus();

		if (compression) {
			connection.getSocketReader().streamTo(decompressor.getInput());
			decompressor.getOutput().streamTo(deserializer.getInput());

			serializer.getOutput().streamTo(compressor.getInput());
			connection.getSocketWriter().streamFrom(compressor.getOutput());
		} else {
			connection.getSocketReader().streamTo(deserializer.getInput());
			connection.getSocketWriter().streamFrom(serializer.getOutput());
		}

		deserializer.getOutput().streamTo(receiver);
		sender.streamTo(serializer.getInput());
	}

	public void sendMessage(RpcMessage message) {
		sendRpcMessage(message);
	}

	public void sendCloseMessage() {
		sendRpcMessage(RpcMessage.of(-1, RpcControlMessage.CLOSE));
	}

	private void sendRpcMessage(RpcMessage message) {
		if (producerStatus == StreamStatus.CLOSED_WITH_ERROR) {
			return;
		}
		assert producerStatus != StreamStatus.END_OF_STREAM;
		downstreamDataReceiver.onData(message);
	}

	public boolean isOverloaded() {
		return producerStatus != StreamStatus.READY;
	}

	public AsyncTcpSocket.EventHandler getSocketEventHandler() {
		return connection;
	}

	public void sendEndOfStream() {
		sender.sendEndOfStream();
	}
}
