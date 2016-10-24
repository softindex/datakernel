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

package io.datakernel.rpc.protocol.stream;

import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.protocol.RpcConnection;
import io.datakernel.rpc.protocol.RpcControlMessage;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocol;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.SimpleStreamConsumer;
import io.datakernel.stream.SimpleStreamProducer;
import io.datakernel.stream.StreamStatus;
import io.datakernel.stream.net.SocketStreamingConnection;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

@SuppressWarnings("unchecked")
final class RpcStreamProtocol implements RpcProtocol {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final RpcConnection rpcConnection;
	private final SimpleStreamProducer<RpcMessage> sender;
	private final SimpleStreamConsumer<RpcMessage> receiver;
	private final SocketStreamingConnection connection;



	protected RpcStreamProtocol(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                          RpcConnection rpcConnection,
	                          BufferSerializer<RpcMessage> messageSerializer,
	                          int defaultPacketSize, int maxPacketSize, boolean compression, boolean server) {
		this.rpcConnection = rpcConnection;

		if (server) {
			sender = SimpleStreamProducer
					.create(eventloop)
					.withStatusListener(new SimpleStreamProducer.StatusListener() {
						@Override
						public void onResumed() {
							receiver.resume();
						}

						@Override
						public void onSuspended() {
							receiver.suspend();
						}

						@Override
						public void onClosedWithError(Exception e) {
							RpcStreamProtocol.this.rpcConnection.onClosedWithError(e);
						}
					});
		} else {
			sender = SimpleStreamProducer
					.create(eventloop)
					.withStatusListener(new SimpleStreamProducer.StatusListener() {
						@Override
						public void onResumed() {
						}

						@Override
						public void onSuspended() {
						}

						@Override
						public void onClosedWithError(Exception e) {
							RpcStreamProtocol.this.rpcConnection.onClosedWithError(e);
						}
					});
		}

		receiver = SimpleStreamConsumer.create(eventloop, new SimpleStreamConsumer.StatusListener() {
			@Override
			public void onEndOfStream() {
				RpcStreamProtocol.this.rpcConnection.onReadEndOfStream();
			}

			@Override
			public void onClosedWithError(Exception e) {
				RpcStreamProtocol.this.rpcConnection.onClosedWithError(e);
			}
		}, rpcConnection);

		StreamBinarySerializer<RpcMessage> serializer = StreamBinarySerializer.create(eventloop, messageSerializer, defaultPacketSize,
				maxPacketSize, 0, true);
		StreamBinaryDeserializer<RpcMessage> deserializer = StreamBinaryDeserializer.create(eventloop, messageSerializer, maxPacketSize);
		this.connection = SocketStreamingConnection.createSocketStreamingConnection(eventloop, asyncTcpSocket);

		if (compression) {
			StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
			StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);
			connection.receiveStreamTo(decompressor.getInput(), ignoreCompletionCallback());
			decompressor.getOutput().streamTo(deserializer.getInput());

			serializer.getOutput().streamTo(compressor.getInput());
			connection.sendStreamFrom(compressor.getOutput(), ignoreCompletionCallback());
		} else {
			connection.receiveStreamTo(deserializer.getInput(), ignoreCompletionCallback());
			connection.sendStreamFrom(serializer.getOutput(), ignoreCompletionCallback());
		}

		deserializer.getOutput().streamTo(receiver);
		sender.streamTo(serializer.getInput());
	}

	@Override
	public void sendMessage(RpcMessage message) {
		sender.send(message);
	}

	@Override
	public void sendCloseMessage() {
		sender.send(RpcMessage.of(-1, RpcControlMessage.CLOSE));
	}

	@Override
	public boolean isOverloaded() {
		return sender.getProducerStatus() == StreamStatus.SUSPENDED;
	}

	@Override
	public AsyncTcpSocket.EventHandler getSocketConnection() {
		return connection;
	}

	@Override
	public void sendEndOfStream() {
		sender.sendEndOfStream();
	}
}
