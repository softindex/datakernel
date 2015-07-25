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

import io.datakernel.annotation.Nullable;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.Node;
import io.datakernel.datagraph.server.command.*;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;

import static io.datakernel.net.SocketSettings.defaultSocketSettings;

/**
 * Client for datagraph server.
 * Sends JSON commands for performing certain actions on server.
 */
public final class DatagraphClient {
	private static final Logger logger = LoggerFactory.getLogger(DatagraphClient.class);

	private final NioEventloop eventloop;
	private final DatagraphSerialization serialization;

	private SocketSettings socketSettings = defaultSocketSettings();

	/**
	 * Constructs a datagraph client that runs in a given event loop and uses the specified DatagraphSerialization object for various serialization purposes.
	 *
	 * @param eventloop     event loop, in which client is to run
	 * @param serialization DatagraphSerialization object used for serialization
	 */
	public DatagraphClient(NioEventloop eventloop, DatagraphSerialization serialization) {
		this.eventloop = eventloop;
		this.serialization = serialization;
	}

	public void connectAndExecute(InetSocketAddress address,
	                              @Nullable final MessagingStarter<DatagraphCommand> starter) {
		eventloop.connect(address, socketSettings, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				StreamMessagingConnection<DatagraphResponse, DatagraphCommand> connection = new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamGsonDeserializer<>(eventloop, serialization.gson, DatagraphResponse.class, 256 * 1024),
						new StreamGsonSerializer<>(eventloop, serialization.gson, DatagraphCommand.class, 256 * 1024, 256 * (1 << 20), 0))
						.addHandler(DatagraphResponseAck.class, new MessagingHandler<DatagraphResponseAck, DatagraphCommand>() {
							@Override
							public void onMessage(DatagraphResponseAck item, Messaging<DatagraphCommand> messaging) {
								messaging.shutdown();
							}
						});
				if (starter != null) {
					connection.addStarter(starter);
				}
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				// TODO (dvolvach)
			}
		});
	}

	public <T> StreamProducer<T> download(InetSocketAddress address, final StreamId streamId, Class<T> type) {
		BufferSerializer<T> serializer = serialization.getSerializer(type);

		final StreamBinaryDeserializer<T> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, serializer, StreamBinarySerializer.MAX_SIZE);
		streamDeserializer.setTag(streamId);

		connectAndExecute(address,
				new MessagingStarter<DatagraphCommand>() {
					@Override
					public void onStart(Messaging<DatagraphCommand> messaging) {
						DatagraphCommandDownload commandDownload = new DatagraphCommandDownload(streamId);
						assert serialization.checkGson(commandDownload, DatagraphCommandDownload.class);
						messaging.sendMessage(commandDownload);
						messaging.shutdownWriter();
						messaging.binarySocketReader().streamTo(streamDeserializer);
					}
				});

		return streamDeserializer;
	}

	public void execute(InetSocketAddress address, Collection<Node> nodes) {
		final DatagraphCommandExecute commandExecute = new DatagraphCommandExecute(new ArrayList<>(nodes));
		assert serialization.checkGson(commandExecute, DatagraphCommandExecute.class);
		connectAndExecute(address,
				new MessagingStarter<DatagraphCommand>() {
					@Override
					public void onStart(Messaging<DatagraphCommand> messaging) {
						messaging.sendMessage(commandExecute);
					}
				}
		);
	}

}
