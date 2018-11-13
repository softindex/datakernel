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

package io.datakernel.datagraph.server;

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
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialQueue;
import io.datakernel.serial.SerialZeroBuffer;
import io.datakernel.serial.net.ByteBufSerializer;
import io.datakernel.serial.net.MessagingWithBinaryStreaming;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.util.MemSize;

import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.serial.net.ByteBufSerializers.ofJson;

/**
 * Server for processing JSON commands.
 */
@SuppressWarnings("rawtypes")
public final class DatagraphServer extends AbstractServer<DatagraphServer> {
	private final DatagraphEnvironment environment;
	private final Map<StreamId, SerialQueue<ByteBuf>> pendingStreams = new HashMap<>();
	private final ByteBufSerializer<DatagraphCommand, DatagraphResponse> serializer;
	private final Map<Class, CommandHandler> handlers = new HashMap<>();

	{
		handlers.put(DatagraphCommandDownload.class, new DownloadCommandHandler());
		handlers.put(DatagraphCommandExecute.class, new ExecuteCommandHandler());
	}

	protected interface CommandHandler<I, O> {
		void onCommand(MessagingWithBinaryStreaming<I, O> messaging, I command);
	}

	// region builders

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
		this.serializer = ofJson(serialization.commandAdapter, serialization.responseAdapter);
	}
	// endregion

	private class DownloadCommandHandler implements CommandHandler<DatagraphCommandDownload, DatagraphResponse> {
		@Override
		public void onCommand(MessagingWithBinaryStreaming<DatagraphCommandDownload, DatagraphResponse> messaging, DatagraphCommandDownload command) {
			StreamId streamId = command.getStreamId();
			SerialQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
			if (forwarder != null) {
				logger.info("onDownload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
			} else {
				logger.info("onDownload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
				forwarder = new SerialZeroBuffer<>();
				pendingStreams.put(streamId, forwarder);
			}
			SerialConsumer<ByteBuf> consumer = messaging.sendBinaryStream();
			forwarder.getSupplier().streamTo(consumer);
			consumer.withAcknowledgement(ack ->
					ack.whenComplete(($, throwable) -> {
						if (throwable != null) {
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
		BufferSerializer<T> serializer = environment.getInstance(DatagraphSerialization.class).getSerializer(type);

		SerialBinarySerializer<T> streamSerializer = SerialBinarySerializer.create(serializer)
				.withInitialBufferSize(MemSize.kilobytes(256))
				.withAutoFlushInterval(Duration.ofSeconds(1));

		SerialQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
		if (forwarder == null) {
			logger.info("onUpload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
			forwarder = new SerialZeroBuffer<>();
			pendingStreams.put(streamId, forwarder);
		} else {
			logger.info("onUpload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
		}
		streamSerializer.getOutput().set(forwarder.getConsumer());
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

	private void doRead(MessagingWithBinaryStreaming<DatagraphCommand, DatagraphResponse> messaging, DatagraphCommand command) {
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
