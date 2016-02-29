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

import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ExceptionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.protocol.FsCommands.Download;
import io.datakernel.protocol.FsCommands.FsCommand;
import io.datakernel.protocol.FsResponses.Err;
import io.datakernel.protocol.FsResponses.ListFiles;
import io.datakernel.protocol.FsResponses.Ready;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.*;
import io.datakernel.stream.processor.StreamByteChunker;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;

import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

public class ClientProtocol {
	@SuppressWarnings("unchecked")
	public static class Builder<T extends Builder<T>> {
		protected final Eventloop eventloop;
		protected int minChunkSize = DEFAULT_MIN_CHUNK_SIZE;
		protected int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
		protected int deserializerBufferSize = DEFAULT_DESERIALIZER_BUFFER_SIZE;
		protected int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
		protected int serializerBufferSize = DEFAULT_SERIALIZER_BUFFER_SIZE;
		protected int serializerMaxMessageSize = DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE;
		protected int serializerFlushDelayMillis = DEFAULT_SERIALIZER_FLUSH_DELAY_MS;
		protected SocketSettings socketSettings = SocketSettings.defaultSocketSettings();

		public Builder(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		public T setMinChunkSize(int minChunkSize) {
			this.minChunkSize = minChunkSize;
			return (T) this;
		}

		public T setMaxChunkSize(int maxChunkSize) {
			this.maxChunkSize = maxChunkSize;
			return (T) this;
		}

		public T setDeserializerBufferSize(int deserializerBufferSize) {
			this.deserializerBufferSize = deserializerBufferSize;
			return (T) this;
		}

		public T setConnectTimeout(int connectTimeout) {
			this.connectTimeout = connectTimeout;
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

		public T setSocketSettings(SocketSettings socketSettings) {
			this.socketSettings = socketSettings;
			return (T) this;
		}

		public ClientProtocol build() {
			return new ClientProtocol(eventloop, minChunkSize, maxChunkSize, connectTimeout,
					socketSettings, serializerBufferSize, serializerMaxMessageSize,
					serializerFlushDelayMillis, deserializerBufferSize);
		}
	}

	protected static abstract class ForwardingConnectCallback implements ConnectCallback {
		private final ExceptionCallback callback;

		protected ForwardingConnectCallback(ExceptionCallback callback) {
			this.callback = callback;
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ClientProtocol.class);

	public static final int DEFAULT_MIN_CHUNK_SIZE = 64 * 1024;
	public static final int DEFAULT_MAX_CHUNK_SIZE = 128 * 1024;
	public static final int DEFAULT_CONNECT_TIMEOUT = 0;
	public static final int DEFAULT_DESERIALIZER_BUFFER_SIZE = 10;
	public static final int DEFAULT_SERIALIZER_BUFFER_SIZE = 256 * 1024;
	public static final int DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE = 256 * (1 << 20);
	public static final int DEFAULT_SERIALIZER_FLUSH_DELAY_MS = 0;

	protected final Eventloop eventloop;
	private final int minChunkSize;
	private final int maxChunkSize;
	private final int connectTimeout;
	private final SocketSettings socketSettings;
	protected final int serializerBufferSize;
	protected final int serializerMaxMessageSize;
	protected final int serializerFlushDelayMillis;
	protected final int deserializerBufferSize;

	// creators
	protected ClientProtocol(Eventloop eventloop, int minChunkSize, int maxChunkSize,
	                         int connectTimeout, SocketSettings socketSettings,
	                         int serializerBufferSize, int serializerMaxMessageSize,
	                         int serializerFlushDelayMillis, int deserializerBufferSize) {
		this.eventloop = checkNotNull(eventloop);

		check(maxChunkSize > minChunkSize, "Max should be bigger then min");
		this.minChunkSize = minChunkSize;
		this.maxChunkSize = maxChunkSize;

		check(connectTimeout >= 0, "Connect timeout should be >= 0");
		this.connectTimeout = connectTimeout;

		this.socketSettings = checkNotNull(socketSettings);

		check(serializerBufferSize > 0, "Serializer buffer size should be positive");
		this.serializerBufferSize = serializerBufferSize;
		check(deserializerBufferSize > 0, "Deserializer buffer size should be positive");
		this.deserializerBufferSize = deserializerBufferSize;
		check(serializerMaxMessageSize > 0, "Serializer max message size should be positive");
		this.serializerMaxMessageSize = serializerMaxMessageSize;

		check(serializerFlushDelayMillis >= 0, "Serializer flush delay should be >= 0");
		this.serializerFlushDelayMillis = serializerFlushDelayMillis;
	}

	public static ClientProtocol newInstance(Eventloop eventloop) {
		return new Builder(eventloop).build();
	}

	public static Builder build(Eventloop eventloop) {
		return new Builder(eventloop);
	}

	// api
	public void upload(final InetSocketAddress address, final String fileName, final StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		connect(address, uploadConnectCallback(fileName, producer, callback));
	}

	public void download(InetSocketAddress address, String fileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback) {
		connect(address, downloadConnectCallback(fileName, startPosition, callback));
	}

	public void delete(InetSocketAddress address, String fileName, CompletionCallback callback) {
		connect(address, deleteConnectCallback(fileName, callback));
	}

	public void list(InetSocketAddress address, ResultCallback<List<String>> callback) {
		connect(address, listFilesConnectCallback(callback));
	}

	// connect callbacks
	protected ConnectCallback uploadConnectCallback(final String fileName,
	                                                final StreamProducer<ByteBuf> producer,
	                                                final CompletionCallback callback) {
		return new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel channel) {
				SocketConnection connection = createConnection(channel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								logger.trace("Send upload command for {}", fileName);
								messaging.sendMessage(new FsCommands.Upload(fileName));
							}
						})
						.addHandler(FsResponses.Ok.class, new MessagingHandler<FsResponses.Ok, FsCommand>() {
							@Override
							public void onMessage(FsResponses.Ok item, final Messaging<FsCommand> messaging) {
								logger.trace("Received ok for {}, start streaming");
								StreamByteChunker byteChunker = new StreamByteChunker(eventloop, minChunkSize, maxChunkSize);
								producer.streamTo(byteChunker.getInput());
								messaging.write(byteChunker.getOutput(), new CompletionCallback() {
									@Override
									public void onComplete() {
										logger.info("Finished streaming {}", fileName);
									}

									@Override
									public void onException(Exception e) {
										logger.info("Failed to stream {}", fileName);
										callback.onException(e);
									}
								});
							}
						})
						.addHandler(FsResponses.Acknowledge.class, new MessagingHandler<FsResponses.Acknowledge, FsCommand>() {
							@Override
							public void onMessage(FsResponses.Acknowledge item, Messaging<FsCommand> messaging) {
								logger.trace("Received acknowledge for {}", fileName);
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(Err.class, new MessagingHandler<Err, FsCommand>() {
							@Override
							public void onMessage(Err item, Messaging<FsCommand> messaging) {
								logger.trace("Failed to upload file {}", fileName);
								messaging.shutdown();
								callback.onException(new Exception(item.msg));
							}
						})
						.addReadExceptionHandler(new MessagingExceptionHandler() {
							@Override
							public void onException(Exception e) {
								logger.trace("Caught exception in stream while uploading file {}", fileName);
								callback.onException(e);
							}
						});
				connection.register();
			}
		};
	}

	protected ConnectCallback downloadConnectCallback(final String fileName, final long startPosition,
	                                                  final ResultCallback<StreamTransformerWithCounter> callback) {
		return new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel channel) {
				SocketConnection connection = createConnection(channel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								logger.trace("Send download command for {}", fileName);
								messaging.sendMessage(new Download(fileName, startPosition));
							}
						})
						.addHandler(Ready.class, new MessagingHandler<Ready, FsCommand>() {
							@Override
							public void onMessage(Ready item, Messaging<FsCommand> messaging) {
								logger.trace("Received acknowledge for {} bytes ready", item.size);
								StreamTransformerWithCounter counter = new StreamTransformerWithCounter(eventloop, item.size - startPosition);
								messaging.read().streamTo(counter.getInput());
								callback.onResult(counter);
								messaging.shutdown();
							}
						})
						.addHandler(Err.class, new MessagingHandler<Err, FsCommand>() {
							@Override
							public void onMessage(Err item, Messaging<FsCommand> messaging) {
								logger.trace("Can't download file {}", item.msg);
								messaging.shutdown();
								Exception e = new Exception(item.msg);
								callback.onException(e);
							}
						})
						.addReadExceptionHandler(new MessagingExceptionHandler() {
							@Override
							public void onException(Exception e) {
								logger.trace("Stream exception while downloading file {}", fileName);
								callback.onException(e);
							}
						});
				connection.register();
			}
		};
	}

	protected ConnectCallback deleteConnectCallback(final String fileName, final CompletionCallback callback) {
		return new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel channel) {
				SocketConnection connection = createConnection(channel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								logger.trace("Send command to delete file {}", fileName);
								messaging.sendMessage(new FsCommands.Delete(fileName));
							}
						})
						.addHandler(FsResponses.Ok.class, new MessagingHandler<FsResponses.Ok, FsCommand>() {
							@Override
							public void onMessage(FsResponses.Ok item, Messaging<FsCommand> messaging) {
								logger.trace("File {} successfully deleted", fileName);
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(Err.class, new MessagingHandler<Err, FsCommand>() {
							@Override
							public void onMessage(Err item, Messaging<FsCommand> messaging) {
								logger.trace("Can't delete file {}, reason: {}", fileName, item.msg);
								messaging.shutdown();
								callback.onException(new Exception(item.msg));
							}
						})
						.addReadExceptionHandler(new MessagingExceptionHandler() {
							@Override
							public void onException(Exception e) {
								logger.trace("Stream exception while deleting file {}", fileName);
								callback.onException(e);
							}
						});
				connection.register();
			}
		};
	}

	protected ConnectCallback listFilesConnectCallback(final ResultCallback<List<String>> callback) {
		return new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel channel) {
				SocketConnection connection = createConnection(channel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								logger.trace("Send command to list files");
								messaging.sendMessage(new FsCommands.ListFiles());
							}
						})
						.addHandler(ListFiles.class, new MessagingHandler<ListFiles, FsCommand>() {
							@Override
							public void onMessage(ListFiles item, Messaging<FsCommand> messaging) {
								logger.trace("Received list of files: {}", item.files.size());
								messaging.shutdown();
								callback.onResult(item.files);
							}
						})
						.addHandler(Err.class, new MessagingHandler<Err, FsCommand>() {
							@Override
							public void onMessage(Err item, Messaging<FsCommand> messaging) {
								logger.trace("Server can't list files {}", item.msg);
								messaging.shutdown();
								callback.onException(new Exception(item.msg));
							}
						})
						.addReadExceptionHandler(new MessagingExceptionHandler() {
							@Override
							public void onException(Exception e) {
								logger.trace("Stream exception while requesting list of files");
								callback.onException(e);
							}
						});
				connection.register();
			}
		};
	}

	protected void connect(SocketAddress address, ConnectCallback callback) {
		eventloop.connect(address, socketSettings, connectTimeout, callback);
	}

	protected StreamMessagingConnection<FsResponses.FsResponse, FsCommand> createConnection(SocketChannel channel) {
		return new StreamMessagingConnection<>(eventloop, channel,
				new StreamGsonDeserializer<>(eventloop, FsResponses.responseGson, FsResponses.FsResponse.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, FsCommands.commandGSON, FsCommand.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis));
	}
}