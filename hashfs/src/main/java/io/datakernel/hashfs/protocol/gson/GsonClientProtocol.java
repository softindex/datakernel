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

package io.datakernel.hashfs.protocol.gson;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.hashfs.protocol.ClientProtocol;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamByteChunker;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class GsonClientProtocol implements ClientProtocol {
	private final NioEventloop eventloop;
	private final int minChunkSize;
	private final int maxChunkSize;
	private final int deserializerBufferSize;
	private final int connectTimeout;
	private final int serializerBufferSize;
	private final int serializerMaxMessageSize;
	private final int serializerFlushDelayMillis;
	private final SocketSettings socketSettings;

	public GsonClientProtocol(NioEventloop eventloop, int minChunkSize, int maxChunkSize, int deserializerBufferSize,
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

	@Override
	public void upload(ServerInfo server, String fileName,
	                   StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		connect(server.getAddress(), defineUpload(server, fileName, producer, callback));
	}

	@Override
	public void download(ServerInfo server, String fileName, StreamConsumer<ByteBuf> consumer, CompletionCallback callback) {
		connect(server.getAddress(), defineDownload(fileName, consumer, callback));
	}

	@Override
	public void delete(ServerInfo server, String fileName, CompletionCallback callback) {
		connect(server.getAddress(), defineDelete(fileName, callback));
	}

	@Override
	public void list(ServerInfo server, ResultCallback<Set<String>> callback) {
		connect(server.getAddress(), defineList(callback));
	}

	@Override
	public void alive(ServerInfo server, ResultCallback<Set<ServerInfo>> callback) {
		connect(server.getAddress(), defineAlive(callback));
	}

	@Override
	public void offer(ServerInfo server, Set<String> forUpload,
	                  Set<String> forDeletion, ResultCallback<Set<String>> callback) {
		connect(server.getAddress(), defineOffer(forUpload, forDeletion, callback));
	}

	private ConnectCallback defineUpload(final ServerInfo server, final String filePath,
	                                     final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								HashFsCommand uploadCommand = new HashFsCommandUpload(filePath);
								messaging.sendMessage(uploadCommand);
							}
						})
						.addHandler(HashFsResponseOk.class, new MessagingHandler<HashFsResponseOk, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseOk item, Messaging<HashFsCommand> messaging) {
								StreamByteChunker byteChunker = new StreamByteChunker(eventloop, minChunkSize, maxChunkSize);
								producer.streamTo(byteChunker.getInput());
								messaging.write(byteChunker.getOutput(), new CompletionCallback() {
									@Override
									public void onComplete() {

									}

									@Override
									public void onException(final Exception e) {
										CompletionCallback transit = new CompletionCallback() {
											@Override
											public void onComplete() {
												callback.onException(e);
											}

											@Override
											public void onException(Exception e1) {
												callback.onException(e);
											}
										};
										commit(server, filePath, transit, false);
									}
								});
							}
						})
						.addHandler(HashFsResponseAcknowledge.class, new MessagingHandler<HashFsResponseAcknowledge, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseAcknowledge item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								commit(server, filePath, callback, true);
							}
						})
						.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
								Exception e = new Exception(item.msg);
								messaging.shutdown();
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

	private ConnectCallback defineDownload(final String filePath, final StreamConsumer<ByteBuf> consumer, final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								HashFsCommandDownload commandDownload = new HashFsCommandDownload(filePath);
								messaging.sendMessage(commandDownload);
							}
						})
						.addHandler(HashFsResponseOk.class, new MessagingHandler<HashFsResponseOk, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseOk item, Messaging<HashFsCommand> messaging) {
								StreamProducer<ByteBuf> producer = messaging.read();
								producer.streamTo(consumer);
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
								Exception e = new Exception(item.msg);
								callback.onException(e);
								messaging.shutdown();
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e)
						.streamTo(consumer);
			}
		};
	}

	private ConnectCallback defineDelete(final String filePath, final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								HashFsCommand commandDelete = new HashFsCommandDelete(filePath);
								messaging.sendMessage(commandDelete);
							}
						})
						.addHandler(HashFsResponseOk.class, new MessagingHandler<HashFsResponseOk, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseOk item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
								Exception e = new Exception(item.msg);
								messaging.shutdown();
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

	private ConnectCallback defineList(final ResultCallback<Set<String>> callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								HashFsCommand commandList = new HashFsCommandList();
								messaging.sendMessage(commandList);
							}
						})
						.addHandler(HashFsResponseListFiles.class, new MessagingHandler<HashFsResponseListFiles, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseListFiles item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onResult(item.files);
							}
						})
						.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
								Exception e = new Exception(item.msg);
								messaging.shutdown();
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

	private ConnectCallback defineAlive(final ResultCallback<Set<ServerInfo>> callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								HashFsCommand checkAlive = new HashFsCommandAlive();
								messaging.sendMessage(checkAlive);
							}
						})
						.addHandler(HashFsResponseListServers.class, new MessagingHandler<HashFsResponseListServers, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseListServers item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onResult(item.servers);
							}
						})
						.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								Exception e = new Exception(item.msg);
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

	private ConnectCallback defineOffer(final Set<String> forUpload, final Set<String> forDeletion,
	                                    final ResultCallback<Set<String>> callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								HashFsCommand offerCommand = new HashFsCommandOffer(forDeletion, forUpload);
								messaging.sendMessage(offerCommand);
							}
						})
						.addHandler(HashFsResponseListFiles.class, new MessagingHandler<HashFsResponseListFiles, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseListFiles item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onResult(item.files);
							}
						})
						.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								Exception e = new Exception(item.msg);
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

	private void commit(ServerInfo server, final String filePath,
	                    final CompletionCallback callback, final boolean success) {
		connect(server.getAddress(), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								HashFsCommandCommit commandCommit = new HashFsCommandCommit(filePath, success);
								messaging.sendMessage(commandCommit);
							}
						})
						.addHandler(HashFsResponseOk.class, new MessagingHandler<HashFsResponseOk, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseOk item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onException(new Exception(item.msg));
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

	private StreamMessagingConnection<HashFsResponse, HashFsCommand> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsResponseSerializer.GSON, HashFsResponse.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, HashFsCommandSerializer.GSON, HashFsCommand.class,
						serializerBufferSize, serializerMaxMessageSize, serializerFlushDelayMillis));
	}
}