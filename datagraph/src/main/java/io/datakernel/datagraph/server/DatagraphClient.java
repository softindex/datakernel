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

package io.datakernel.datagraph.server;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ConnectCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.Node;
import io.datakernel.datagraph.server.command.DatagraphCommand;
import io.datakernel.datagraph.server.command.DatagraphCommandDownload;
import io.datakernel.datagraph.server.command.DatagraphCommandExecute;
import io.datakernel.datagraph.server.command.DatagraphResponse;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.datakernel.stream.net.MessagingSerializers.ofGson;

/**
 * Client for datagraph server.
 * Sends JSON commands for performing certain actions on server.
 */
public final class DatagraphClient {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final DatagraphSerialization serialization;
	private final MessagingSerializer<DatagraphResponse, DatagraphCommand> serializer;

	private SocketSettings socketSettings = SocketSettings.create();

	/**
	 * Constructs a datagraph client that runs in a given event loop and uses the specified DatagraphSerialization object for various serialization purposes.
	 *
	 * @param eventloop     event loop, in which client is to run
	 * @param serialization DatagraphSerialization object used for serialization
	 */
	public DatagraphClient(Eventloop eventloop, DatagraphSerialization serialization) {
		this.eventloop = eventloop;
		this.serialization = serialization;
		this.serializer = ofGson(serialization.gson, DatagraphResponse.class, serialization.gson, DatagraphCommand.class);
	}

	public void connectAndExecute(InetSocketAddress address, ConnectCallback callback) {
		eventloop.connect(address, callback);
	}

	private class DownloadConnectCallback extends ConnectCallback {
		private final StreamId streamId;
		private final StreamConsumer<ByteBuf> consumer;
		private final CompletionCallback callback;

		public DownloadConnectCallback(StreamId streamId, StreamConsumer<ByteBuf> consumer, CompletionCallback callback) {
			this.streamId = streamId;
			this.consumer = consumer;
			this.callback = callback;
		}

		@Override
		public void onConnect(SocketChannel socketChannel) {
			AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, socketSettings);
			final MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
			DatagraphCommandDownload commandDownload = new DatagraphCommandDownload(streamId);

			messaging.send(commandDownload, new CompletionCallback() {
				@Override
				public void onComplete() {
					messaging.receiveBinaryStreamTo(consumer, new CompletionCallback() {
						@Override
						public void onComplete() {
							messaging.close();
							callback.setComplete();
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							callback.setException(e);
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.setException(e);
				}
			});
			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocket.register();
		}

		@Override
		public void onException(Exception e) {
			callback.setException(e);
		}
	}

	private class ExecuteConnectCallback extends ConnectCallback {
		private final List<Node> nodes;
		private final CompletionCallback callback;

		private ExecuteConnectCallback(List<Node> nodes, CompletionCallback callback) {
			this.nodes = nodes;
			this.callback = callback;
		}

		@Override
		public void onConnect(SocketChannel socketChannel) {
			AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, socketSettings);
			final MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
			DatagraphCommandExecute commandExecute = new DatagraphCommandExecute(nodes);
			messaging.send(commandExecute, new CompletionCallback() {
				@Override
				public void onComplete() {
					messaging.close();
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.setException(e);
				}
			});
			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocket.register();
		}

		@Override
		public void onException(Exception e) {
			callback.setException(e);
		}
	}

	public <T> StreamProducer<T> download(InetSocketAddress address, final StreamId streamId, Class<T> type) {
		BufferSerializer<T> serializer = serialization.getSerializer(type);
		StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(eventloop, serializer,
				StreamBinarySerializer.MAX_SIZE);
		connectAndExecute(address, new DownloadConnectCallback(streamId, deserializer.getInput(), new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Downloading stream {} completed", streamId);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to download stream {}", streamId, e);
			}
		}));
		return deserializer.getOutput();
	}

	public void execute(InetSocketAddress address, final Collection<Node> nodes) {
		connectAndExecute(address, new ExecuteConnectCallback(new ArrayList<>(nodes), new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Execute command sent to nodes {}", nodes);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to send execute command to nodes {}", nodes, e);
			}
		}));
	}
}
