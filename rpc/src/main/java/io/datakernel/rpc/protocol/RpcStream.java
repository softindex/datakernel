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
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.net.SocketStreamingConnection;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;

import static io.datakernel.stream.DataStreams.stream;

@SuppressWarnings("unchecked")
public final class RpcStream {
	public interface Listener extends StreamDataReceiver<RpcMessage> {
		void onClosedWithError(Throwable exception);

		void onReadEndOfStream();
	}

	private Listener listener;
	private final AbstractStreamProducer<RpcMessage> sender;
	private final AbstractStreamConsumer<RpcMessage> receiver;
	private final SocketStreamingConnection connection;

	private boolean ready;
	private StreamDataReceiver<RpcMessage> downstreamDataReceiver;

	public RpcStream(AsyncTcpSocket asyncTcpSocket,
	                 BufferSerializer<RpcMessage> messageSerializer,
	                 int defaultPacketSize, int maxPacketSize,
	                 int autoFlushIntervalMillis, boolean compression, boolean server) {

		connection = SocketStreamingConnection.create(asyncTcpSocket);

		if (server) {
			sender = new AbstractStreamProducer<RpcMessage>() {
				@Override
				protected void onProduce(StreamDataReceiver<RpcMessage> dataReceiver) {
					RpcStream.this.downstreamDataReceiver = dataReceiver;
					receiver.getProducer().produce(RpcStream.this.listener);
					ready = true;
				}

				@Override
				protected void onSuspended() {
					receiver.getProducer().suspend();
					ready = false;
				}

				@Override
				protected void onError(Throwable t) {
					RpcStream.this.listener.onClosedWithError(t);
					ready = false;
				}
			};
		} else {
			sender = new AbstractStreamProducer<RpcMessage>() {
				@Override
				protected void onProduce(StreamDataReceiver<RpcMessage> dataReceiver) {
					RpcStream.this.downstreamDataReceiver = dataReceiver;
					ready = true;
				}

				@Override
				protected void onSuspended() {
					ready = false;
				}

				@Override
				protected void onError(Throwable t) {
					RpcStream.this.listener.onClosedWithError(t);
					ready = false;
				}
			};
		}

		receiver = new AbstractStreamConsumer<RpcMessage>() {
			@Override
			protected void onStarted() {
				getProducer().produce(RpcStream.this.listener);
			}

			@Override
			protected void onEndOfStream() {
				RpcStream.this.listener.onReadEndOfStream();
			}

			@Override
			protected void onError(Throwable t) {
			}
		};

		StreamBinarySerializer<RpcMessage> serializer = StreamBinarySerializer.create(messageSerializer)
				.withDefaultBufferSize(defaultPacketSize)
				.withMaxMessageSize(maxPacketSize)
				.withAutoFlush(autoFlushIntervalMillis)
				.withSkipSerializationErrors();
		StreamBinaryDeserializer<RpcMessage> deserializer = StreamBinaryDeserializer.create(messageSerializer);

		StreamLZ4Decompressor decompressor;
		StreamLZ4Compressor compressor;
		if (compression) {
			compressor = StreamLZ4Compressor.fastCompressor();
			decompressor = StreamLZ4Decompressor.create();
			stream(connection.getSocketReader(), decompressor.getInput());
			stream(decompressor.getOutput(), deserializer.getInput());

			stream(serializer.getOutput(), compressor.getInput());
			stream(compressor.getOutput(), connection.getSocketWriter());
		} else {
			stream(connection.getSocketReader(), deserializer.getInput());
			stream(serializer.getOutput(), connection.getSocketWriter());
		}

		stream(deserializer.getOutput(), receiver);
		stream(sender, serializer.getInput());
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
			downstreamDataReceiver.onData(message);
		}
	}

	public boolean isOverloaded() {
		return !ready;
	}

	public AsyncTcpSocket.EventHandler getSocketEventHandler() {
		return connection;
	}

	public void sendEndOfStream() {
		sender.sendEndOfStream();
	}
}
