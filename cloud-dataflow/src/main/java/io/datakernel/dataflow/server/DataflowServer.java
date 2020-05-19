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
import io.datakernel.csp.binary.ByteBufsCodec;
import io.datakernel.csp.net.Messaging;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.dataflow.di.BinarySerializerModule.BinarySerializerLocator;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.graph.TaskContext;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphCommandDownload;
import io.datakernel.dataflow.server.command.DatagraphCommandExecute;
import io.datakernel.dataflow.server.command.DatagraphResponse;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.di.ResourceLocator;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AbstractServer;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Server for processing JSON commands.
 */
@SuppressWarnings("rawtypes")
public final class DataflowServer extends AbstractServer<DataflowServer> {
	private final Map<StreamId, ChannelQueue<ByteBuf>> pendingStreams = new HashMap<>();
	private final Map<Class, CommandHandler> handlers = new HashMap<>();

	private final ByteBufsCodec<DatagraphCommand, DatagraphResponse> codec;
	private final BinarySerializerLocator serializers;
	private final ResourceLocator environment;

	{
		handlers.put(DatagraphCommandDownload.class, new DownloadCommandHandler());
		handlers.put(DatagraphCommandExecute.class, new ExecuteCommandHandler());
	}

	protected interface CommandHandler<I, O> {
		void onCommand(Messaging<I, O> messaging, I command);
	}

	public DataflowServer(Eventloop eventloop, ByteBufsCodec<DatagraphCommand, DatagraphResponse> codec, BinarySerializerLocator serializers, ResourceLocator environment) {
		super(eventloop);
		this.codec = codec;
		this.serializers = serializers;
		this.environment = environment;
	}

	private class DownloadCommandHandler implements CommandHandler<DatagraphCommandDownload, DatagraphResponse> {
		@Override
		public void onCommand(Messaging<DatagraphCommandDownload, DatagraphResponse> messaging, DatagraphCommandDownload command) {
			if (logger.isTraceEnabled()) {
				logger.trace("Processing onDownload: {}, {}", command, messaging);
			}
			StreamId streamId = command.getStreamId();
			ChannelQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
			if (forwarder != null) {
				logger.info("onDownload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
			} else {
				forwarder = new ChannelZeroBuffer<>();
				pendingStreams.put(streamId, forwarder);
				logger.info("onDownload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
				messaging.receive()
						.whenException(() -> {
							ChannelQueue<ByteBuf> removed = pendingStreams.remove(streamId);
							if (removed != null) {
								logger.info("onDownload: removing {}, pending downloads: {}", streamId, pendingStreams.size());
							}
						});
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
		public void onCommand(Messaging<DatagraphCommandExecute, DatagraphResponse> messaging, DatagraphCommandExecute command) {
			TaskContext task = new TaskContext(environment);
			try {
				for (Node node : command.getNodes()) {
					node.createAndBind(task);
				}
			} catch (Exception e) {
				logger.error("Failed to createAndBind task: {}", command, e);
				sendResponse(messaging, e);
				return;
			}

			task.execute()
					.whenComplete(($, throwable) -> {
						if (throwable == null) {
							logger.info("Task executed successfully: {}", command);
						} else {
							logger.error("Failed to execute task: {}", command, throwable);
						}
						sendResponse(messaging, throwable);
					});

			messaging.receive()
					.whenException(() -> {
						if (!task.isExecuted()) {
							logger.error("Client disconnected. Canceling task: {}", command);
							task.cancel();
						}
					});
		}

		private void sendResponse(Messaging<DatagraphCommandExecute, DatagraphResponse> messaging, @Nullable Throwable throwable) {
			String error = null;
			if (throwable != null) {
				error = throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
			}
			messaging.send(new DatagraphResponse(error))
					.whenComplete(messaging::close);
		}
	}

	public <T> StreamConsumer<T> upload(StreamId streamId, Class<T> type) {
		BinarySerializer<T> serializer = serializers.get(type);

		ChannelSerializer<T> streamSerializer = ChannelSerializer.create(serializer)
				.withInitialBufferSize(MemSize.kilobytes(256))
				.withAutoFlushInterval(Duration.ZERO)
				.withExplicitEndOfStream();

		ChannelQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
		if (forwarder == null) {
			forwarder = new ChannelZeroBuffer<>();
			pendingStreams.put(streamId, forwarder);
			logger.info("onUpload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
		} else {
			logger.info("onUpload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
		}
		streamSerializer.getOutput().set(forwarder.getConsumer());
		streamSerializer.getAcknowledgement()
				.whenException(() -> {
					ChannelQueue<ByteBuf> removed = pendingStreams.remove(streamId);
					if (removed != null) {
						logger.info("onUpload: removing {}, pending downloads: {}", streamId, pendingStreams.size());
						removed.close();
					}
				});
		return streamSerializer;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		Messaging<DatagraphCommand, DatagraphResponse> messaging = MessagingWithBinaryStreaming.create(socket, codec);
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
	private void doRead(Messaging<DatagraphCommand, DatagraphResponse> messaging, DatagraphCommand command) {
		CommandHandler handler = handlers.get(command.getClass());
		if (handler == null) {
			messaging.close();
			logger.error("missing handler for " + command);
		} else {
			handler.onCommand(messaging, command);
		}
	}
}
