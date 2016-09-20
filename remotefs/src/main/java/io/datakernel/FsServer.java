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

package io.datakernel;

import com.google.gson.Gson;
import io.datakernel.FsCommands.*;
import io.datakernel.FsResponses.*;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public abstract class FsServer<S extends FsServer<S>> extends AbstractServer<S> {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected final FileManager fileManager;
	private MessagingSerializer<FsCommand, FsResponse> serializer = ofGson(getCommandGSON(), FsCommand.class, getResponseGson(), FsResponse.class);

	protected final Map<Class, MessagingHandler> handlers;

	// region creators & builder methods
	protected FsServer(Eventloop eventloop, FileManager fileManager) {
		super(eventloop);
		this.fileManager = fileManager;
		this.handlers = createHandlers();
	}

	protected FsServer(Eventloop eventloop,
	                   ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                   boolean acceptOnce, Collection<InetSocketAddress> listenAddresses,
	                   InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                   SSLContext sslContext, ExecutorService sslExecutor,
	                   Collection<InetSocketAddress> sslListenAddresses,
	                   S previousInstance) {
		super(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses);
		this.fileManager = previousInstance.fileManager;
		this.handlers = previousInstance.handlers;
	}
	// endregion

	// abstract core methods
	protected abstract void upload(String filePath, ResultCallback<StreamConsumer<ByteBuf>> callback);

	protected abstract void download(String filePath, long startPosition, ResultCallback<StreamProducer<ByteBuf>> callback);

	protected abstract void delete(String filePath, CompletionCallback callback);

	protected abstract void list(ResultCallback<List<String>> callback);

	// set up connection
	@Override
	protected final EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		final MessagingWithBinaryStreaming<FsCommand, FsResponse> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
		messaging.receive(new ReceiveMessageCallback<FsCommand>() {
			@Override
			public void onReceive(FsCommand msg) {
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

	private void doRead(MessagingWithBinaryStreaming<FsCommand, FsResponse> messaging, FsCommand item) {
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
		return FsResponses.responseGson;
	}

	protected Gson getCommandGSON() {
		return FsCommands.commandGSON;
	}

	protected interface MessagingHandler<I, O> {
		void onMessage(MessagingWithBinaryStreaming<I, O> messaging, I item);
	}

	private Map<Class, MessagingHandler> createHandlers() {
		Map<Class, MessagingHandler> map = new HashMap<>();
		map.put(Upload.class, new UploadMessagingHandler());
		map.put(Download.class, new DownloadMessagingHandler());
		map.put(Delete.class, new DeleteMessagingHandler());
		map.put(ListFiles.class, new ListFilesMessagingHandler());
		return map;
	}

	// handler classes
	private class UploadMessagingHandler implements MessagingHandler<Upload, FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreaming<Upload, FsResponse> messaging, final Upload item) {
			upload(item.filePath, new ResultCallback<StreamConsumer<ByteBuf>>() {
				@Override
				public void onResult(StreamConsumer<ByteBuf> result) {
					messaging.receiveBinaryStreamTo(result, new ForwardingCompletionCallback(this) {
						@Override
						public void onComplete() {
							logger.trace("read all bytes for {}", item.filePath);
							messaging.send(new Acknowledge(), ignoreCompletionCallback());
							messaging.sendEndOfStream(ignoreCompletionCallback());
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

	private class DownloadMessagingHandler implements MessagingHandler<Download, FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreaming<Download, FsResponse> messaging, final Download item) {
			fileManager.size(item.filePath, new ResultCallback<Long>() {
				@Override
				public void onResult(final Long size) {
					if (size < 0) {
						messaging.send(new Err("File not found"), ignoreCompletionCallback());
						messaging.sendEndOfStream(ignoreCompletionCallback());
					} else {
						messaging.send(new Ready(size), new ForwardingCompletionCallback(this) {
							@Override
							public void onComplete() {
								download(item.filePath, item.startPosition, new ForwardingResultCallback<StreamProducer<ByteBuf>>(this) {
									@Override
									public void onResult(final StreamProducer<ByteBuf> result) {
										messaging.sendBinaryStreamFrom(result, new SimpleCompletionCallback() {
											@Override
											protected void onCompleteOrException() {
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
					messaging.send(new Err(e.getMessage()), ignoreCompletionCallback());
					messaging.sendEndOfStream(ignoreCompletionCallback());
				}
			});
		}
	}

	private class DeleteMessagingHandler implements MessagingHandler<Delete, FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreaming<Delete, FsResponse> messaging, final Delete item) {
			delete(item.filePath, new CompletionCallback() {
				@Override
				public void onComplete() {
					messaging.send(new Ok(), ignoreCompletionCallback());
					messaging.sendEndOfStream(ignoreCompletionCallback());
				}

				@Override
				public void onException(Exception e) {
					messaging.send(new Err(e.getMessage()), ignoreCompletionCallback());
					messaging.sendEndOfStream(ignoreCompletionCallback());
				}
			});
		}
	}

	private class ListFilesMessagingHandler implements MessagingHandler<ListFiles, FsResponse> {
		@Override
		public void onMessage(final MessagingWithBinaryStreaming<ListFiles, FsResponse> messaging, ListFiles item) {
			list(new ResultCallback<List<String>>() {
				@Override
				public void onResult(List<String> result) {
					messaging.send(new ListOfFiles(result), ignoreCompletionCallback());
					messaging.sendEndOfStream(ignoreCompletionCallback());
				}

				@Override
				public void onException(Exception e) {
					messaging.send(new Err(e.getMessage()), ignoreCompletionCallback());
					messaging.sendEndOfStream(ignoreCompletionCallback());
				}
			});
		}
	}
}