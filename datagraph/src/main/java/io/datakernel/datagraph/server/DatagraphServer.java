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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.graph.TaskContext;
import io.datakernel.datagraph.node.Node;
import io.datakernel.datagraph.server.command.DatagraphCommand;
import io.datakernel.datagraph.server.command.DatagraphCommandDownload;
import io.datakernel.datagraph.server.command.DatagraphCommandExecute;
import io.datakernel.datagraph.server.command.DatagraphResponse;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreamingConnection;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.stream.net.MessagingSerializers.ofGson;

/**
 * Server for processing JSON commands.
 */
public final class DatagraphServer extends AbstractServer<DatagraphServer> {
	private static final Logger logger = LoggerFactory.getLogger(DatagraphServer.class);

	private final DatagraphEnvironment environment;
	private final Map<StreamId, StreamForwarder<ByteBuf>> pendingStreams = new HashMap<>();
	private final MessagingSerializer<DatagraphCommand, DatagraphResponse> serializer;
	private final Map<Class, CommandHandler> handlers = new HashMap<>();

	{
		handlers.put(DatagraphCommandDownload.class, new DownloadCommandHandler());
		handlers.put(DatagraphCommandExecute.class, new ExecuteCommandHandler());
	}

	protected interface CommandHandler<I, O> {
		void onCommand(MessagingWithBinaryStreamingConnection<I, O> messaging, I command);
	}

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
		DatagraphSerialization serialization = environment.getInstance(DatagraphSerialization.class);
		this.serializer = ofGson(serialization.gson, DatagraphCommand.class, serialization.gson, DatagraphResponse.class);
	}

	private class DownloadCommandHandler implements CommandHandler<DatagraphCommandDownload, DatagraphResponse> {
		@Override
		public void onCommand(final MessagingWithBinaryStreamingConnection<DatagraphCommandDownload, DatagraphResponse> messaging, DatagraphCommandDownload command) {
			StreamId streamId = command.streamId;
			StreamForwarder<ByteBuf> forwarder = pendingStreams.remove(streamId);
			if (forwarder != null) {
				logger.info("onDownload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
			} else {
				logger.info("onDownload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
				forwarder = new StreamForwarder<>(eventloop);
				pendingStreams.put(streamId, forwarder);
			}
			messaging.sendBinaryStreamFrom(forwarder.getOutput(), new CompletionCallback() {
				@Override
				public void onComplete() {
					messaging.close();
				}

				@Override
				public void onException(Exception e) {
					logger.warn("Exception occurred while trying to send data");
					messaging.close();
				}
			});
		}
	}

	private class ExecuteCommandHandler implements CommandHandler<DatagraphCommandExecute, DatagraphResponse> {
		@Override
		public void onCommand(MessagingWithBinaryStreamingConnection<DatagraphCommandExecute, DatagraphResponse> messaging, DatagraphCommandExecute command) {
			messaging.close();
			TaskContext taskContext = new TaskContext(eventloop, DatagraphEnvironment.extend(environment));
			for (Node node : command.getNodes()) {
				node.createAndBind(taskContext);
			}
			taskContext.wireAll();
		}
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
	protected final AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		final MessagingWithBinaryStreamingConnection<DatagraphCommand, DatagraphResponse> messaging = new MessagingWithBinaryStreamingConnection<>(eventloop, asyncTcpSocket, serializer);
		messaging.receive(new Messaging.ReceiveMessageCallback<DatagraphCommand>() {
			@Override
			public void onReceive(DatagraphCommand msg) {
				doRead(messaging, msg);
			}

			@Override
			public void onReceiveEndOfStream() {
				logger.warn("unexpected end of stream");
				messaging.close();
			}

			@Override
			public void onException(Exception e) {
				logger.error("received error while trying to read", e);
				messaging.close();
			}
		});
		return messaging;
	}

	private void doRead(MessagingWithBinaryStreamingConnection<DatagraphCommand, DatagraphResponse> messaging, DatagraphCommand command) {
		CommandHandler handler = handlers.get(command.getClass());
		if (handler == null) {
			messaging.close();
			logger.error("missing handler for " + command);
		} else {
			//noinspection unchecked
			handler.onCommand(messaging, command);
		}
	}
}
