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

package io.datakernel.simplefs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.simplefs.FsResponse.Ok;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.net.*;
import io.datakernel.stream.processor.StreamByteChunker;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Set;

final class GsonClientProtocol {
	public static final class Builder {
		private final NioEventloop eventloop;
		private int minChunkSize = DEFAULT_MIN_CHUNK_SIZE;
		private int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
		private int deserializerBufferSize = DEFAULT_DESERIALIZER_BUFFER_SIZE;
		private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
		private int serializerBufferSize = DEFAULT_SERIALIZER_BUFFER_SIZE;
		private int serializerMaxMessageSize = DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE;
		private int serializerFlushDelayMillis = DEFAULT_SERIALIZER_FLUSH_DELAY_MS;
		private SocketSettings socketSettings = SocketSettings.defaultSocketSettings();

		private Builder(NioEventloop eventloop) {
			this.eventloop = eventloop;
		}

		public Builder setMinChunkSize(int minChunkSize) {
			this.minChunkSize = minChunkSize;
			return this;
		}

		public Builder setMaxChunkSize(int maxChunkSize) {
			this.maxChunkSize = maxChunkSize;
			return this;
		}

		public Builder setDeserializerBufferSize(int deserializerBufferSize) {
			this.deserializerBufferSize = deserializerBufferSize;
			return this;
		}

		public Builder setConnectTimeout(int connectTimeout) {
			this.connectTimeout = connectTimeout;
			return this;
		}

		public Builder setSerializerBufferSize(int serializerBufferSize) {
			this.serializerBufferSize = serializerBufferSize;
			return this;
		}

		public Builder setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			this.serializerMaxMessageSize = serializerMaxMessageSize;
			return this;
		}

		public Builder setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			this.serializerFlushDelayMillis = serializerFlushDelayMillis;
			return this;
		}

		public Builder setSocketSettings(SocketSettings socketSettings) {
			this.socketSettings = socketSettings;
			return this;
		}

		public GsonClientProtocol build() {
			return new GsonClientProtocol(eventloop, minChunkSize, maxChunkSize, deserializerBufferSize,
					connectTimeout, serializerBufferSize, serializerMaxMessageSize,
					serializerFlushDelayMillis, socketSettings);
		}
	}

	private static int DEFAULT_MIN_CHUNK_SIZE = 64 * 1024;
	private static int DEFAULT_MAX_CHUNK_SIZE = 128 * 1024;
	private static int DEFAULT_CONNECT_TIMEOUT = 0;
	private static int DEFAULT_DESERIALIZER_BUFFER_SIZE = 10;
	private static int DEFAULT_SERIALIZER_BUFFER_SIZE = 256 * 1024;
	private static int DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE = 256 * (1 << 20);
	private static int DEFAULT_SERIALIZER_FLUSH_DELAY_MS = 0;

	private final NioEventloop eventloop;
	private final int minChunkSize;
	private final int maxChunkSize;
	private final int deserializerBufferSize;
	private final int connectTimeout;
	private final int serializerBufferSize;
	private final int serializerMaxMessageSize;
	private final int serializerFlushDelayMillis;
	private final SocketSettings socketSettings;

	private GsonClientProtocol(NioEventloop eventloop, int minChunkSize, int maxChunkSize, int deserializerBufferSize,
	                           int connectTimeout, int serializerBufferSize, int serializerMaxMessageSize,
	                           int serializerFlushDelayMillis, SocketSettings socketSettings) {
		this.eventloop = eventloop;
		this.minChunkSize = minChunkSize;
		this.maxChunkSize = maxChunkSize;
		this.deserializerBufferSize = deserializerBufferSize;
		this.connectTimeout = connectTimeout;
		this.serializerBufferSize = serializerBufferSize;
		this.serializerMaxMessageSize = serializerMaxMessageSize;
		this.serializerFlushDelayMillis = serializerFlushDelayMillis;
		this.socketSettings = socketSettings;
	}

	public static GsonClientProtocol createInstance(NioEventloop eventloop) {
		return buildInstance(eventloop).build();
	}

	public static GsonClientProtocol.Builder buildInstance(NioEventloop eventloop) {
		return new Builder(eventloop);
	}

	public void upload(InetSocketAddress address, String fileName, StreamProducer<ByteBuf> producer,
	                   CompletionCallback callback) {
		connect(address, defineUpload(address, fileName, producer, callback));
	}

	public void download(InetSocketAddress address, String fileName, StreamConsumer<ByteBuf> consumer,
	                     CompletionCallback callback) {
		connect(address, defineDownload(fileName, consumer, callback));
	}

	public void delete(InetSocketAddress address, String fileName, CompletionCallback callback) {
		connect(address, defineDelete(fileName, callback));
	}

	public void list(InetSocketAddress address, ResultCallback<Set<String>> callback) {
		connect(address, defineList(callback));
	}

	protected ConnectCallback defineUpload(final InetSocketAddress address, final String fileName,
	                                       final StreamProducer<ByteBuf> producer,
	                                       final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								FsCommand uploadCommand = new FsCommand.Upload(fileName);
								messaging.sendMessage(uploadCommand);
							}
						})
						.addHandler(Ok.class, new MessagingHandler<Ok, FsCommand>() {
							@Override
							public void onMessage(Ok item, final Messaging<FsCommand> messaging) {
								StreamByteChunker byteChunker = new StreamByteChunker(eventloop, minChunkSize, maxChunkSize);
								producer.streamTo(byteChunker.getInput());
								messaging.write(byteChunker.getOutput(), new CompletionCallback() {
									@Override
									public void onComplete() {

									}

									@Override
									public void onException(final Exception e) {
										messaging.shutdown();
										commit(address, fileName, false, callback);
									}
								});
							}
						})
						.addHandler(FsResponse.Acknowledge.class, new MessagingHandler<FsResponse.Acknowledge, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Acknowledge item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								commit(address, fileName, true, callback);
							}
						})
						.addHandler(FsResponse.Error.class, new MessagingHandler<FsResponse.Error, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Error item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								Exception e = new Exception(item.msg);
								callback.onException(e);
							}
						}).addReadException(new MessagingException() {
							@Override
							public void onException(Exception e) {
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		};
	}

	protected ConnectCallback defineDownload(final String fileName, final StreamConsumer<ByteBuf> consumer,
	                                         final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								FsCommand commandDownload = new FsCommand.Download(fileName);
								messaging.sendMessage(commandDownload);
							}
						})
						.addHandler(FsResponse.Ready.class, new MessagingHandler<FsResponse.Ready, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Ready item, Messaging<FsCommand> messaging) {
								Util.CounterTransformer counter = new Util.CounterTransformer(eventloop, item.size);
								messaging.read().streamTo(counter.getInput());
								counter.getOutput().streamTo(consumer);
								messaging.shutdown();
							}
						})
						.addHandler(FsResponse.Error.class, new MessagingHandler<FsResponse.Error, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Error item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								Exception e = new Exception(item.msg);
								StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
								callback.onException(e);
							}
						})
						.addReadException(new MessagingException() {
							@Override
							public void onException(Exception e) {
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		};
	}

	protected ConnectCallback defineDelete(final String fileName, final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								FsCommand commandDelete = new FsCommand.Delete(fileName);
								messaging.sendMessage(commandDelete);
							}
						})
						.addHandler(FsResponse.Ok.class, new MessagingHandler<FsResponse.Ok, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Ok item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(FsResponse.Error.class, new MessagingHandler<FsResponse.Error, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Error item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								Exception e = new Exception(item.msg);
								callback.onException(e);
							}
						})
						.addReadException(new MessagingException() {
							@Override
							public void onException(Exception e) {
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		};
	}

	protected ConnectCallback defineList(final ResultCallback<Set<String>> callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								FsCommand commandList = new FsCommand.List();
								messaging.sendMessage(commandList);
							}
						})
						.addHandler(FsResponse.ListFiles.class, new MessagingHandler<FsResponse.ListFiles, FsCommand>() {
							@Override
							public void onMessage(FsResponse.ListFiles item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								callback.onResult(item.files);
							}
						})
						.addHandler(FsResponse.Error.class, new MessagingHandler<FsResponse.Error, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Error item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								Exception e = new Exception(item.msg);
								callback.onException(e);
							}
						})
						.addReadException(new MessagingException() {
							@Override
							public void onException(Exception e) {
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		};
	}

	protected void commit(InetSocketAddress address, final String fileName, final boolean success,
	                      final CompletionCallback callback) {
		connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								FsCommand.Commit commandCommit = new FsCommand.Commit(fileName, success);
								messaging.sendMessage(commandCommit);
							}
						})
						.addHandler(FsResponse.Ok.class, new MessagingHandler<FsResponse.Ok, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Ok item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								if (success) {
									callback.onComplete();
								} else {
									callback.onException(new Exception("Can't send file"));
								}
							}
						})
						.addHandler(FsResponse.Error.class, new MessagingHandler<FsResponse.Error, FsCommand>() {
							@Override
							public void onMessage(FsResponse.Error item, Messaging<FsCommand> messaging) {
								messaging.shutdown();
								callback.onException(new Exception(item.msg));
							}
						})
						.addReadException(new MessagingException() {
							@Override
							public void onException(Exception e) {
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	private void connect(SocketAddress address, ConnectCallback callback) {
		eventloop.connect(address, socketSettings, connectTimeout, callback);
	}

	private StreamMessagingConnection<FsResponse, FsCommand> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, FsResponse.getGSON(), FsResponse.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, FsCommand.getGSON(), FsCommand.class,
						serializerBufferSize, serializerMaxMessageSize, serializerFlushDelayMillis));
	}
}