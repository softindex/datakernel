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
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

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

	// public api
	public void upload(String fileName, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		fileManager.save(fileName, new ForwardingResultCallback<StreamFileWriter>(callback) {
			@Override
			public void onResult(StreamFileWriter result) {
				callback.setResult(result);
			}
		});
	}

	public void download(String fileName, long startPosition, final ResultCallback<StreamProducer<ByteBuf>> callback) {
		fileManager.get(fileName, startPosition, new ForwardingResultCallback<StreamFileReader>(callback) {
			@Override
			public void onResult(StreamFileReader result) {
				callback.setResult(result);
			}
		});
	}

	public void delete(String fileName, CompletionCallback callback) {
		fileManager.delete(fileName, callback);
	}

	protected void list(ResultCallback<List<String>> callback) {
		fileManager.scan(callback);
	}

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

	private void doRead(MessagingWithBinaryStreaming<RemoteFsCommands.FsCommand, RemoteFsResponses.FsResponse> messaging, RemoteFsCommands.FsCommand item) {
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
		void onMessage(MessagingWithBinaryStreaming<I, O> messaging, I item);
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
		public void onMessage(final MessagingWithBinaryStreaming<RemoteFsCommands.Upload, RemoteFsResponses.FsResponse> messaging, final RemoteFsCommands.Upload item) {
			upload(item.filePath, new ResultCallback<StreamConsumer<ByteBuf>>() {
				@Override
				public void onResult(StreamConsumer<ByteBuf> result) {
					messaging.receiveBinaryStreamTo(result, new ForwardingCompletionCallback(this) {
						@Override
						public void onComplete() {
							logger.trace("read all bytes for {}", item.filePath);
							messaging.send(new RemoteFsResponses.Acknowledge(), IgnoreCompletionCallback.create());
							messaging.sendEndOfStream(IgnoreCompletionCallback.create());
						}
					});
				}

				@Override
				public void onException(Exception exception) {
					messaging.close();
				}
			});
		}
	}

	private class DownloadMessagingHandler implements MessagingHandler<RemoteFsCommands.Download, RemoteFsResponses.FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreaming<RemoteFsCommands.Download, RemoteFsResponses.FsResponse> messaging, final RemoteFsCommands.Download item) {
			fileManager.size(item.filePath, new ResultCallback<Long>() {
				@Override
				public void onResult(final Long size) {
					if (size < 0) {
						messaging.send(new RemoteFsResponses.Err("File not found"), IgnoreCompletionCallback.create());
						messaging.sendEndOfStream(IgnoreCompletionCallback.create());
					} else {
						messaging.send(new RemoteFsResponses.Ready(size), new ForwardingCompletionCallback(this) {
							@Override
							public void onComplete() {
								download(item.filePath, item.startPosition, new ForwardingResultCallback<StreamProducer<ByteBuf>>(this) {
									@Override
									public void onResult(final StreamProducer<ByteBuf> result) {
										messaging.sendBinaryStreamFrom(result, new CompletionCallback() {
											@Override
											protected void onException(Exception e) {
												onCompleteOrException();
											}

											@Override
											protected void onComplete() {
												onCompleteOrException();
											}

											void onCompleteOrException() {
												messaging.close();
											}
										});
									}
								});
							}
						});
					}
				}

				@Override
				public void onException(Exception e) {
					messaging.send(new RemoteFsResponses.Err(e.getMessage()), IgnoreCompletionCallback.create());
					messaging.sendEndOfStream(IgnoreCompletionCallback.create());
				}
			});
		}
	}

	private class DeleteMessagingHandler implements MessagingHandler<RemoteFsCommands.Delete, RemoteFsResponses.FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreaming<RemoteFsCommands.Delete, RemoteFsResponses.FsResponse> messaging, final RemoteFsCommands.Delete item) {
			delete(item.filePath, new CompletionCallback() {
				@Override
				public void onComplete() {
					messaging.send(new RemoteFsResponses.Ok(), IgnoreCompletionCallback.create());
					messaging.sendEndOfStream(IgnoreCompletionCallback.create());
				}

				@Override
				public void onException(Exception e) {
					messaging.send(new RemoteFsResponses.Err(e.getMessage()), IgnoreCompletionCallback.create());
					messaging.sendEndOfStream(IgnoreCompletionCallback.create());
				}
			});
		}
	}

	private class ListFilesMessagingHandler implements MessagingHandler<RemoteFsCommands.ListFiles, RemoteFsResponses.FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreaming<RemoteFsCommands.ListFiles, RemoteFsResponses.FsResponse> messaging, RemoteFsCommands.ListFiles item) {
			list(new ResultCallback<List<String>>() {
				@Override
				public void onResult(List<String> result) {
					messaging.send(new RemoteFsResponses.ListOfFiles(result), IgnoreCompletionCallback.create());
					messaging.sendEndOfStream(IgnoreCompletionCallback.create());
				}

				@Override
				public void onException(Exception e) {
					messaging.send(new RemoteFsResponses.Err(e.getMessage()), IgnoreCompletionCallback.create());
					messaging.sendEndOfStream(IgnoreCompletionCallback.create());
				}
			});
		}
	}
}