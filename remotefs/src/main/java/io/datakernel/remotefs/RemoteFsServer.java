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

package io.datakernel.remotefs;

import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public final class RemoteFsServer extends AbstractServer<RemoteFsServer> {
	protected final FileManager fileManager;
	private MessagingSerializer<RemoteFsCommands.FsCommand, RemoteFsResponses.FsResponse> serializer = ofGson(getCommandGSON(), RemoteFsCommands.FsCommand.class, getResponseGson(), RemoteFsResponses.FsResponse.class);

	private final Map<Class, MessagingHandler> handlers;

	// region creators & builder methods
	private RemoteFsServer(Eventloop eventloop, FileManager fileManager) {
		super(eventloop);
		this.fileManager = fileManager;
		this.handlers = createHandlers();
	}

	public static RemoteFsServer create(Eventloop eventloop, ExecutorService executor, Path storage) {
		return new RemoteFsServer(eventloop, FileManager.create(eventloop, executor, storage));
	}
	// endregion

	// set up connection
	@Override
	protected final EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		final MessagingWithBinaryStreaming<RemoteFsCommands.FsCommand, RemoteFsResponses.FsResponse> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
		messaging.receive(new ReceiveMessageCallback<RemoteFsCommands.FsCommand>() {
			@Override
			public void onReceive(RemoteFsCommands.FsCommand msg) {
				logger.trace("received {}", msg);
				doRead(messaging, msg);
			}

			@Override
			public void onReceiveEndOfStream() {
				logger.warn("unexpected end of stream");
				messaging.close();
			}

			@Override
			public void onException(Exception e) {
				logger.error("received error while reading", e);
				messaging.close();
			}
		});
		return messaging;
	}

	private void doRead(Messaging<RemoteFsCommands.FsCommand, RemoteFsResponses.FsResponse> messaging, RemoteFsCommands.FsCommand item) {
		MessagingHandler handler = handlers.get(item.getClass());
		if (handler == null) {
			messaging.close();
			logger.error("missing handler for " + item);
		} else {
			//noinspection unchecked
			handler.onMessage(messaging, item);
		}
	}

	protected Gson getResponseGson() {
		return RemoteFsResponses.responseGson;
	}

	protected Gson getCommandGSON() {
		return RemoteFsCommands.commandGSON;
	}

	protected interface MessagingHandler<I, O> {
		void onMessage(Messaging<I, O> messaging, I item);
	}

	private Map<Class, MessagingHandler> createHandlers() {
		Map<Class, MessagingHandler> map = new HashMap<>();
		map.put(RemoteFsCommands.Upload.class, new UploadMessagingHandler());
		map.put(RemoteFsCommands.Download.class, new DownloadMessagingHandler());
		map.put(RemoteFsCommands.Delete.class, new DeleteMessagingHandler());
		map.put(RemoteFsCommands.ListFiles.class, new ListFilesMessagingHandler());
		return map;
	}

	// handler classes
	private class UploadMessagingHandler implements MessagingHandler<RemoteFsCommands.Upload, RemoteFsResponses.FsResponse> {
		@Override
		public void onMessage(final Messaging<RemoteFsCommands.Upload, RemoteFsResponses.FsResponse> messaging, final RemoteFsCommands.Upload item) {
			logger.trace("uploading {}", item.filePath);
			fileManager.save(item.filePath).whenComplete((fileWriter, throwable) -> {
				if (throwable == null) {
					messaging.receiveBinaryStream().streamTo(fileWriter);
					fileWriter.getFlushStage().whenComplete(($, throwable1) -> {
						if (throwable1 == null) {
							logger.trace("read all bytes for {}", item.filePath);
							messaging.send(new RemoteFsResponses.Acknowledge());
							messaging.sendEndOfStream();
						} else {
							messaging.close();
						}
					});
				} else {
					messaging.close();
				}
			});
		}
	}

	private class DownloadMessagingHandler implements MessagingHandler<RemoteFsCommands.Download, RemoteFsResponses.FsResponse> {

		private <T, U extends Throwable> BiConsumer<T, U> errorHandlingConsumer(
				final Messaging<RemoteFsCommands.Download, RemoteFsResponses.FsResponse> messaging,
				BiConsumer<T, U> resultConsumer) {

			return (t, u) -> {
				if (u == null) {
					resultConsumer.accept(t, null);
				} else {
					onException(messaging, u);
				}
			};
		}

		public void onException(final Messaging<RemoteFsCommands.Download, RemoteFsResponses.FsResponse> messaging, Throwable throwable) {
			messaging.send(new RemoteFsResponses.Err(throwable.getMessage()));
			messaging.sendEndOfStream();
		}

		@Override
		public void onMessage(final Messaging<RemoteFsCommands.Download, RemoteFsResponses.FsResponse> messaging, final RemoteFsCommands.Download item) {
			fileManager.size(item.filePath).whenComplete(errorHandlingConsumer(messaging, (size, throwable) -> {
				if (size >= 0) {
					messaging.send(new RemoteFsResponses.Ready(size)).whenComplete(errorHandlingConsumer(messaging, (aVoid, throwable1) ->
							fileManager.get(item.filePath, item.startPosition).whenComplete(errorHandlingConsumer(messaging, (fileReader, throwable2) -> {
								StreamConsumerWithResult<ByteBuf, Void> consumer = messaging.sendBinaryStream();
								fileReader.streamTo(consumer);
								consumer.getResult().whenComplete(($, throwable3) -> messaging.close());
							}))));
				} else {
					onException(messaging, new Throwable("File not found"));
				}
			}));
		}
	}

	private class DeleteMessagingHandler implements MessagingHandler<RemoteFsCommands.Delete, RemoteFsResponses.FsResponse> {
		@Override
		public void onMessage(final Messaging<RemoteFsCommands.Delete, RemoteFsResponses.FsResponse> messaging, final RemoteFsCommands.Delete item) {
			fileManager.delete(item.filePath).whenComplete((aVoid, throwable) -> {
				messaging.send(throwable == null
						? new RemoteFsResponses.Ok()
						: new RemoteFsResponses.Err(throwable.getMessage()));
				messaging.sendEndOfStream();
			});
		}
	}

	private class ListFilesMessagingHandler implements MessagingHandler<RemoteFsCommands.ListFiles, RemoteFsResponses.FsResponse> {
		@Override
		public void onMessage(final Messaging<RemoteFsCommands.ListFiles, RemoteFsResponses.FsResponse> messaging, RemoteFsCommands.ListFiles item) {
			fileManager.scanAsync().whenComplete((strings, throwable) -> {
				messaging.send(throwable == null
						? new RemoteFsResponses.ListOfFiles(strings)
						: new RemoteFsResponses.Err(throwable.getMessage()));
				messaging.sendEndOfStream();
			});
		}
	}
}