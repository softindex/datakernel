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
import io.datakernel.datagraph.graph.TaskContext;
import io.datakernel.datagraph.node.Node;
import io.datakernel.datagraph.server.command.DatagraphCommand;
import io.datakernel.datagraph.server.command.DatagraphCommandDownload;
import io.datakernel.datagraph.server.command.DatagraphCommandExecute;
import io.datakernel.datagraph.server.command.DatagraphResponse;
import io.datakernel.eventloop.AbstractEventloopServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

/**
 * Server for processing JSON commands.
 */
public final class DatagraphServer extends AbstractEventloopServer<DatagraphServer> {
	private static final Logger logger = LoggerFactory.getLogger(DatagraphServer.class);

	private final DatagraphEnvironment environment;

	private final Map<StreamId, StreamForwarder<ByteBuf>> pendingStreams = new HashMap<>();

	/**
	 * Constructs a datagraph server with the given environment that runs in the specified event loop.
	 *
	 * @param eventloop   event loop which runs the server
	 * @param environment datagraph environment to use
	 */
	public DatagraphServer(Eventloop eventloop, DatagraphEnvironment environment) {
		super(eventloop);
		this.environment = DatagraphEnvironment.extend(environment)
				.set(DatagraphServer.class, this);
	}

	private void onDownload(final DatagraphCommandDownload item, Messaging<DatagraphResponse> messaging) {
		messaging.shutdownReader();
		StreamId streamId = item.streamId;
		StreamForwarder<ByteBuf> forwarder = pendingStreams.remove(streamId);
		if (forwarder != null) {
			logger.info("onDownload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
		} else {
			logger.info("onDownload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
			forwarder = new StreamForwarder<>(eventloop);
			pendingStreams.put(streamId, forwarder);
		}
		messaging.write(forwarder.getOutput(), ignoreCompletionCallback());
	}

	private void onExecute(DatagraphCommandExecute item, Messaging<DatagraphResponse> messaging) {
		messaging.shutdown();
		TaskContext taskContext = new TaskContext(eventloop, DatagraphEnvironment.extend(environment));
		for (Node node : item.getNodes()) {
			node.createAndBind(taskContext);
		}
		taskContext.wireAll();
	}

	public <T> StreamConsumer<T> upload(final StreamId streamId, Class<T> type) {
		BufferSerializer<T> serializer = environment.getInstance(DatagraphSerialization.class).getSerializer(type);

		StreamBinarySerializer<T> streamSerializer = new StreamBinarySerializer<>(eventloop, serializer, 256 * 1024, StreamBinarySerializer.MAX_SIZE, 1000, false);
		streamSerializer.setTag(streamId);

		StreamForwarder<ByteBuf> forwarder = pendingStreams.remove(streamId);
		if (forwarder != null) {
			logger.info("onUpload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
		} else {
			logger.info("onUpload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
			forwarder = new StreamForwarder<>(eventloop);
			pendingStreams.put(streamId, forwarder);
		}
		streamSerializer.getOutput().streamTo(forwarder.getInput());
		return streamSerializer.getInput();
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		DatagraphSerialization serialization = environment.getInstance(DatagraphSerialization.class);
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, serialization.gson, DatagraphCommand.class, 256 * 1024),
				new StreamGsonSerializer<>(eventloop, serialization.gson, DatagraphResponse.class, 256 * 1024, 256 * (1 << 20), 0))
				.addHandler(DatagraphCommandDownload.class, new MessagingHandler<DatagraphCommandDownload, DatagraphResponse>() {
					@Override
					public void onMessage(DatagraphCommandDownload item, Messaging<DatagraphResponse> messaging) {
						onDownload(item, messaging);
					}

				})
				.addHandler(DatagraphCommandExecute.class, new MessagingHandler<DatagraphCommandExecute, DatagraphResponse>() {
					@Override
					public void onMessage(DatagraphCommandExecute item, Messaging<DatagraphResponse> messaging) {
						onExecute(item, messaging);
					}
				});
	}

}
