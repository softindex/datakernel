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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
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
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

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

	public CompletionStage<SocketChannel> connectAndExecute(InetSocketAddress address) {
		return eventloop.connect(address);
	}

	private class DownloadHandler {
		private final StreamId streamId;
		private final StreamConsumer<ByteBuf> consumer;
		private final SettableStage<Void> completionStage;

		public DownloadHandler(StreamId streamId, StreamConsumer<ByteBuf> consumer) {
			this.streamId = streamId;
			this.consumer = consumer;
			this.completionStage = SettableStage.create();
		}

		public void onConnect(SocketChannel socketChannel) {
			AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, socketSettings);
			final MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
			DatagraphCommandDownload commandDownload = new DatagraphCommandDownload(streamId);

			messaging.send(commandDownload).whenComplete(($, throwable) -> {
				if (throwable == null) {
					StreamProducerWithResult<ByteBuf, Void> producer = messaging.receiveBinaryStream();
					producer.streamTo(consumer);
					producer.getResult().whenComplete(($1, throwable1) -> {
						messaging.close();
						AsyncCallbacks.forwardTo(completionStage, null, throwable1);
					});
				} else {
					messaging.close();
					completionStage.setException(throwable);
				}
			});
			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocket.register();
		}

		public void onException(Throwable e) {
			completionStage.setException(e);
		}

		public CompletionStage<Void> getCompletionStage() {
			return completionStage;
		}
	}

	private class ExecuteHandler {
		private final List<Node> nodes;
		private final SettableStage<Void> completionStage = SettableStage.create();

		private ExecuteHandler(List<Node> nodes) {
			this.nodes = nodes;
		}

		public void onConnect(SocketChannel socketChannel) {
			AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, socketSettings);
			final MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
			DatagraphCommandExecute commandExecute = new DatagraphCommandExecute(nodes);

			messaging.send(commandExecute).whenComplete(($, throwable) -> {
				if (throwable == null) {
					messaging.close();
					completionStage.set(null);
				} else {
					messaging.close();
					completionStage.setException(throwable);
				}
			});

			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocket.register();
		}

		public void onException(Throwable e) {
			completionStage.setException(e);
		}

		public SettableStage<Void> getCompletionStage() {
			return completionStage;
		}
	}

	public <T> StreamProducer<T> download(InetSocketAddress address, final StreamId streamId, Class<T> type) {
		BufferSerializer<T> serializer = serialization.getSerializer(type);
		StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(eventloop, serializer);
		final DownloadHandler downloadhandler = new DownloadHandler(streamId, deserializer.getInput());
		downloadhandler.getCompletionStage().whenComplete(($, throwable) -> {
			if (throwable == null) logger.info("Downloading stream {} completed", streamId);
			else logger.error("Failed to download stream {}", streamId, throwable);
		});

		connectAndExecute(address).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				downloadhandler.onConnect(socketChannel);
			} else {
				downloadhandler.onException(throwable);
			}
		});
		return deserializer.getOutput();
	}

	public void execute(InetSocketAddress address, final Collection<Node> nodes) {
		final ExecuteHandler executeHandler = new ExecuteHandler(new ArrayList<>(nodes));
		executeHandler.getCompletionStage().whenComplete((aVoid, throwable) -> {
			if (throwable == null) logger.info("Execute command sent to nodes {}", nodes);
			else logger.error("Failed to send execute command to nodes {}", nodes, throwable);
		});

		connectAndExecute(address).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				executeHandler.onConnect(socketChannel);
			} else {
				executeHandler.onException(throwable);
			}
		});
	}
}
