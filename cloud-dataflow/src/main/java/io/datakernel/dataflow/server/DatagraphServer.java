/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.dataflow.server;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.MemSize;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.binary.ByteBufSerializer;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.graph.TaskContext;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphCommandDownload;
import io.datakernel.dataflow.server.command.DatagraphCommandExecute;
import io.datakernel.dataflow.server.command.DatagraphResponse;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AbstractServer;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.serializer.BinarySerializer;

import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Server for processing JSON commands.
 */
@SuppressWarnings("rawtypes")
public final class DatagraphServer extends AbstractServer<DatagraphServer> {
	private final DatagraphEnvironment environment;
	private final Map<StreamId, ChannelQueue<ByteBuf>> pendingStreams = new HashMap<>();
	private final ByteBufSerializer<DatagraphCommand, DatagraphResponse> serializer;
	private final Map<Class, CommandHandler> handlers = new HashMap<>();

	{
		handlers.put(DatagraphCommandDownload.class, new DownloadCommandHandler());
		handlers.put(DatagraphCommandExecute.class, new ExecuteCommandHandler());
	}

	protected interface CommandHandler<I, O> {
		void onCommand(MessagingWithBinaryStreaming<I, O> messaging, I command);
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
				.with(DatagraphServer.class, this);
		DatagraphSerialization serialization = environment.getInstance(DatagraphSerialization.class);
		this.serializer = ByteBufSerializer.ofJsonCodec(serialization.getCommandCodec(), serialization.getResponseCodec());
	}

	private class DownloadCommandHandler implements CommandHandler<DatagraphCommandDownload, DatagraphResponse> {
		@Override
		public void onCommand(MessagingWithBinaryStreaming<DatagraphCommandDownload, DatagraphResponse> messaging, DatagraphCommandDownload command) {
			StreamId streamId = command.getStreamId();
			ChannelQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
			if (forwarder != null) {
				logger.info("onDownload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
			} else {
				logger.info("onDownload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
				forwarder = new ChannelZeroBuffer<>();
				pendingStreams.put(streamId, forwarder);
			}
			ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream();
			forwarder.getSupplier().streamTo(consumer);
			consumer.withAcknowledgement(ack ->
					ack.whenComplete(($, e) -> {
						if (e != null) {
							logger.warn("Exception occurred while trying to send data");
						}
						messaging.close();
					}));
		}
	}

	private class ExecuteCommandHandler implements CommandHandler<DatagraphCommandExecute, DatagraphResponse> {
		@Override
		public void onCommand(MessagingWithBinaryStreaming<DatagraphCommandExecute, DatagraphResponse> messaging, DatagraphCommandExecute command) {
			messaging.close();
			TaskContext taskContext = new TaskContext(eventloop, DatagraphEnvironment.extend(environment));
			for (Node node : command.getNodes()) {
				node.createAndBind(taskContext);
			}
			taskContext.wireAll();
		}
	}

	public <T> StreamConsumer<T> upload(StreamId streamId, Class<T> type) {
		BinarySerializer<T> serializer = environment.getInstance(DatagraphSerialization.class).getSerializer(type);

		ChannelSerializer<T> streamSerializer = ChannelSerializer.create(serializer)
				.withInitialBufferSize(MemSize.kilobytes(256))
				.withAutoFlushInterval(Duration.ofSeconds(1));

		ChannelQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
		if (forwarder == null) {
			logger.info("onUpload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
			forwarder = new ChannelZeroBuffer<>();
			pendingStreams.put(streamId, forwarder);
		} else {
			logger.info("onUpload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
		}
		streamSerializer.getOutput()
				.set(forwarder.getConsumer());
		return streamSerializer;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<DatagraphCommand, DatagraphResponse> messaging = MessagingWithBinaryStreaming.create(socket, serializer);
		messaging.receive()
				.whenResult(msg -> {
					if (msg != null) {
						doRead(messaging, msg);
					} else {
						logger.warn("unexpected end of stream");
						messaging.close();
					}
				})
				.whenException(e -> {
					logger.error("received error while trying to read", e);
					messaging.close();
				});
	}

	@SuppressWarnings("unchecked")
	private void doRead(MessagingWithBinaryStreaming<DatagraphCommand, DatagraphResponse> messaging, DatagraphCommand command) {
		CommandHandler handler = handlers.get(command.getClass());
		if (handler == null) {
			messaging.close();
			logger.error("missing handler for " + command);
		} else {
			handler.onCommand(messaging, command);
		}
	}
}
