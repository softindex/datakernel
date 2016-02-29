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

package io.datakernel.protocol;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.protocol.FsCommands.*;
import io.datakernel.protocol.FsResponses.Acknowledge;
import io.datakernel.protocol.FsResponses.Err;
import io.datakernel.protocol.FsResponses.FsResponse;
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

import static io.datakernel.protocol.FsCommands.*;
import static io.datakernel.protocol.FsResponses.responseGson;

public class ServerProtocol<S extends FsServer> extends AbstractServer<ServerProtocol<S>> {
	@SuppressWarnings("unchecked")
	public static class Builder<T extends Builder, S extends FsServer> {
		protected final Eventloop eventloop;
		protected int deserializerBufferSize = DEFAULT_DESERIALIZER_BUFFER_SIZE;
		protected int serializerBufferSize = DEFAULT_SERIALIZER_BUFFER_SIZE;
		protected int serializerMaxMessageSize = DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE;
		protected int serializerFlushDelayMillis = DEFAULT_SERIALIZER_FLUSH_DELAY_MS;

		protected Builder(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		public T setDeserializerBufferSize(int deserializerBufferSize) {
			this.deserializerBufferSize = deserializerBufferSize;
			return (T) this;
		}

		public T setSerializerBufferSize(int serializerBufferSize) {
			this.serializerBufferSize = serializerBufferSize;
			return (T) this;
		}

		public T setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			this.serializerMaxMessageSize = serializerMaxMessageSize;
			return (T) this;
		}

		public T setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			this.serializerFlushDelayMillis = serializerFlushDelayMillis;
			return (T) this;
		}

		public ServerProtocol<S> build() {
			return new ServerProtocol<>(eventloop, serializerBufferSize,
					serializerMaxMessageSize, serializerFlushDelayMillis, deserializerBufferSize);
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ServerProtocol.class);

	public static final int DEFAULT_DESERIALIZER_BUFFER_SIZE = 10;
	public static final int DEFAULT_SERIALIZER_BUFFER_SIZE = 256 * 1024;
	public static final int DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE = 256 * (1 << 20);
	public static final int DEFAULT_SERIALIZER_FLUSH_DELAY_MS = 0;

	protected final int serializerBufferSize;
	protected final int serializerMaxMessageSize;
	protected final int serializerFlushDelayMillis;
	protected final int deserializerBufferSize;

	protected S server;

	// creators
	protected ServerProtocol(Eventloop eventloop, int serializerBufferSize,
	                         int serializerMaxMessageSize, int serializerFlushDelayMillis,
	                         int deserializerBufferSize) {
		super(eventloop);
		this.serializerBufferSize = serializerBufferSize;
		this.serializerMaxMessageSize = serializerMaxMessageSize;
		this.serializerFlushDelayMillis = serializerFlushDelayMillis;
		this.deserializerBufferSize = deserializerBufferSize;
	}

	public static <S extends FsServer> ServerProtocol<S> newInstance(Eventloop eventloop) {
		return new Builder<Builder, S>(eventloop).build();
	}

	public static <T extends Builder, S extends FsServer> Builder<T, S> build(Eventloop eventloop) {
		return new Builder<>(eventloop);
	}

	// required to link the server this protocol instance would serve
	public void wire(S server) {
		this.server = server;
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, commandGSON, FsCommand.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, responseGson, FsResponse.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis))
				.addHandler(Upload.class, defineUploadHandler())
				.addHandler(Download.class, defineDownloadHandler())
				.addHandler(Delete.class, defineDeleteHandler())
				.addHandler(ListFiles.class, defineListFilesHandler());
	}

	// handlers
	protected MessagingHandler<Upload, FsResponse> defineUploadHandler() {
		return new MessagingHandler<Upload, FsResponse>() {
			@Override
			public void onMessage(final Upload item, final Messaging<FsResponse> messaging) {
				messaging.sendMessage(new FsResponses.Ok());
				server.upload(item.filePath, messaging.read(), new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Finished streaming data for {}", item.filePath);
						messaging.sendMessage(new Acknowledge());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						logger.error("Exception while streaming data for {}", item.filePath);
						messaging.sendMessage(new Err(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	protected MessagingHandler<Download, FsResponse> defineDownloadHandler() {
		return new MessagingHandler<Download, FsResponse>() {
			@Override
			public void onMessage(final Download item, final Messaging<FsResponse> messaging) {
				server.fileSize(item.filePath, new ResultCallback<Long>() {
					@Override
					public void onResult(Long size) {
						if (size < 0) {
							logger.trace("Responding err to file download: {}. File not found", item.filePath);
							messaging.sendMessage(new Err("File not found"));
							messaging.shutdown();
							return;
						}

						logger.trace("Responding ok to file download: {}. File size {}", item.filePath, size);
						messaging.sendMessage(new FsResponses.Ready(size));

						// preventing output stream from being explicitly closed
						messaging.shutdownReader();

						server.download(item.filePath, item.startPosition, new ResultCallback<StreamProducer<ByteBuf>>() {
							@Override
							public void onResult(StreamProducer<ByteBuf> result) {
								logger.trace("Opened stream {}", result);
								messaging.write(result, new CompletionCallback() {
									@Override
									public void onComplete() {
										logger.info("File data for {} has been send", item.filePath);
										messaging.shutdownWriter();
									}

									@Override
									public void onException(Exception e) {
										logger.error("Failed to send data for: {}", item.filePath, e);
										messaging.shutdownWriter();
									}
								});
							}

							@Override
							public void onException(Exception e) {
								logger.error("Failed to get stream for: {}", item.filePath, e);
								messaging.shutdown();
							}
						});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Unable to retrieve size for file {}", item.filePath);
						messaging.sendMessage(new Err(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	protected MessagingHandler<ListFiles, FsResponse> defineListFilesHandler() {
		return new MessagingHandler<ListFiles, FsResponse>() {
			@Override
			public void onMessage(ListFiles item, final Messaging<FsResponse> messaging) {
				server.list(new ResultCallback<List<String>>() {
					@Override
					public void onResult(List<String> result) {
						logger.trace("Sending list of files to server: {}", result.size());
						messaging.sendMessage(new FsResponses.ListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						logger.trace("Can't list files {}", e.getMessage());
						messaging.sendMessage(new Err(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	protected MessagingHandler<Delete, FsResponse> defineDeleteHandler() {
		return new MessagingHandler<Delete, FsResponse>() {
			@Override
			public void onMessage(final Delete item, final Messaging<FsResponse> messaging) {
				server.delete(item.filePath, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.trace("Responding ok to file {} deletion", item.filePath);
						messaging.sendMessage(new FsResponses.Ok());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						logger.trace("Responding err to file {} deletion", item.filePath);
						messaging.sendMessage(new Err(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}
}