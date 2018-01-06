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
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import static io.datakernel.stream.DataStreams.stream;
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

	public <T> CompletionStage<StreamProducer<T>> download(InetSocketAddress address, StreamId streamId, Class<T> type) {
		return eventloop.connect(address).thenCompose(socketChannel -> {
			AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, socketSettings);
			MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);
			DatagraphCommandDownload commandDownload = new DatagraphCommandDownload(streamId);

			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocket.register();

			return messaging.send(commandDownload)
					.thenApply($ -> {
						StreamProducerWithResult<ByteBuf, Void> producer = messaging.receiveBinaryStream();
						BufferSerializer<T> serializer = serialization.getSerializer(type);
						StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(serializer);

						stream(producer, deserializer.getInput())
								.thenAccept($_ -> messaging.close());
						return deserializer.getOutput();
					});
		});
	}

	public CompletionStage<Void> execute(InetSocketAddress address, Collection<Node> nodes) {
		return eventloop.connect(address).thenCompose(socketChannel -> {
			AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, socketSettings);
			MessagingWithBinaryStreaming<DatagraphResponse, DatagraphCommand> messaging = MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocket.register();

			DatagraphCommandExecute commandExecute = new DatagraphCommandExecute(new ArrayList<>(nodes));
			return messaging.send(commandExecute)
					.thenAccept($ -> messaging.sendEndOfStream());
		});
	}
}
