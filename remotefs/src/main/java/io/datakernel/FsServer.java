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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

public abstract class FsServer<S extends FsServer<S>> extends AbstractServer<S> {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	private int deserializerBufferSize = 256 * (1 << 10);   // 256kb
	private int serializerBufferSize = 256 * (1 << 10);     // 256kb
	private int serializerMaxMessageSize = 256 * (1 << 20); // 256mb
	private int serializerFlushDelayMs = 0;

	protected final FileManager fileManager;

	// creators & builder methods
	protected FsServer(Eventloop eventloop, FileManager fileManager) {
		super(eventloop);
		this.fileManager = fileManager;
	}

	public S setSerializerBufferSize(int serializerBufferSize) {
		checkState(serializerBufferSize > 0, "Serializer buffer size: %d should be positive", serializerBufferSize);
		this.serializerBufferSize = serializerBufferSize;
		return self();
	}

	public S setSerializerMaxMessageSize(int serializerMaxMessageSize) {
		checkState(serializerMaxMessageSize > 0, "Serializer max message size: %d should be positive", serializerMaxMessageSize);
		this.serializerMaxMessageSize = serializerMaxMessageSize;
		return self();
	}

	public S setSerializerFlushDelayMs(int serializerFlushDelayMs) {
		checkState(serializerFlushDelayMs >= 0, "Serializer flush delay millis: %d should not be negative", serializerFlushDelayMs);
		this.serializerFlushDelayMs = serializerFlushDelayMs;
		return self();
	}

	public S setDeserializerBufferSize(int deserializerBufferSize) {
		checkState(deserializerBufferSize > 0, "Deserializer buffer size: %d should be positive", deserializerBufferSize);
		this.deserializerBufferSize = deserializerBufferSize;
		return self();
	}

	// set up connection
	@Override
	protected final SocketConnection createConnection(SocketChannel socketChannel) {
		StreamMessagingConnection<FsCommand, FsResponse> conn = new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, getCommandGSON(), FsCommand.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, getResponseGson(), FsResponse.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMs));
		addHandlers(conn);
		return conn;
	}

	protected Gson getResponseGson() {
		return FsResponses.responseGson;
	}

	protected Gson getCommandGSON() {
		return FsCommands.commandGSON;
	}

	protected void addHandlers(StreamMessagingConnection<FsCommand, FsResponse> conn) {
		conn.addHandler(Upload.class, new UploadMessagingHandler());
		conn.addHandler(Download.class, new DownloadMessagingHandler());
		conn.addHandler(Delete.class, new DeleteMessagingHandler());
		conn.addHandler(ListFiles.class, new ListFilesMessagingHandler());
	}

	// handler classes
	private class UploadMessagingHandler implements MessagingHandler<Upload, FsResponse> {
		@Override
		public void onMessage(final Upload item, final Messaging<FsResponse> messaging) {
			logger.info("received command to upload file: {}", item.filePath);
			messaging.sendMessage(new Ok());
			upload(item.filePath, messaging.read(), new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("succeed to upload file: {}", item.filePath);
					messaging.sendMessage(new Acknowledge());
					messaging.shutdown();
				}

				@Override
				public void onException(Exception e) {
					logger.error("failed to upload file: {}", item.filePath, e);
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	private class DownloadMessagingHandler implements MessagingHandler<Download, FsResponse> {
		@Override
		public void onMessage(final Download item, final Messaging<FsResponse> messaging) {
			logger.info("received command to download file: {}", item.filePath);
			fileManager.size(item.filePath, new ResultCallback<Long>() {
				@Override
				public void onResult(final Long size) {
					if (size < 0) {
						logger.warn("missing file: {}", item.filePath);
						messaging.sendMessage(new Err("File not found"));
						messaging.shutdown();
					} else {
						// preventing output stream from being explicitly closed
						messaging.shutdownReader();
						download(item.filePath, item.startPosition, new ResultCallback<StreamProducer<ByteBuf>>() {
							@Override
							public void onResult(StreamProducer<ByteBuf> result) {
								logger.info("opened stream for file: {}", item.filePath);
								messaging.sendMessage(new Ready(size));
								messaging.write(result, new CompletionCallback() {
									@Override
									public void onComplete() {
										logger.info("succeed to stream file: {}", item.filePath);
										messaging.shutdownWriter();
									}

									@Override
									public void onException(Exception e) {
										logger.error("failed to stream file: {}", item.filePath, e);
										messaging.shutdownWriter();
									}
								});
							}

							@Override
							public void onException(Exception e) {
								logger.error("failed to open stream for file: {}", item.filePath, e);
								messaging.shutdown();
							}
						});
					}
				}

				@Override
				public void onException(Exception e) {
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	private class DeleteMessagingHandler implements MessagingHandler<Delete, FsResponse> {
		@Override
		public void onMessage(final Delete item, final Messaging<FsResponse> messaging) {
			logger.info("received command to delete file: {}", item.filePath);
			delete(item.filePath, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("succeed to delete file: {}", item.filePath);
					messaging.sendMessage(new Ok());
					messaging.shutdown();
				}

				@Override
				public void onException(Exception e) {
					logger.error("failed to delete file: {}", item.filePath, e);
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	private class ListFilesMessagingHandler implements MessagingHandler<ListFiles, FsResponse> {
		@Override
		public void onMessage(ListFiles item, final Messaging<FsResponse> messaging) {
			logger.info("received command to list files");
			list(new ResultCallback<List<String>>() {
				@Override
				public void onResult(List<String> result) {
					logger.info("succeed to list files: {}", result.size());
					messaging.sendMessage(new ListOfFiles(result));
					messaging.shutdown();
				}

				@Override
				public void onException(Exception e) {
					logger.error("failed to list files", e);
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	// abstract core methods
	protected abstract void upload(String filePath, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	protected abstract void download(String filePath, long startPosition, ResultCallback<StreamProducer<ByteBuf>> callback);

	protected abstract void delete(String filePath, CompletionCallback callback);

	protected abstract void list(ResultCallback<List<String>> callback);
}