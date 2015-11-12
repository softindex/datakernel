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

package io.datakernel.remotefs.protocol.gson;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.remotefs.RfsConfig;
import io.datakernel.remotefs.ServerInfo;
import io.datakernel.remotefs.protocol.ClientProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.*;
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

	public static GsonClientProtocol createInstance(NioEventloop eventloop, RfsConfig config) {
		return new GsonClientProtocol(eventloop,
				config.getMinChunkSize(),
				config.getMaxChunkSize(),
				config.getDeserializerBufferSize(),
				config.getConnectTimeout(),
				config.getSerializerBufferSize(),
				config.getSerializerMaxMessageSize(),
				config.getSerializerFlushDelayMillis(),
				config.getSocketSettings());
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

	private ConnectCallback defineUpload(final ServerInfo server, final String fileName,
	                                     final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<Command>() {
							@Override
							public void onStart(Messaging<Command> messaging) {
								Command uploadCommand = new CommandUpload(fileName);
								messaging.sendMessage(uploadCommand);
							}
						})
						.addHandler(ResponseOk.class, new MessagingHandler<ResponseOk, Command>() {
							@Override
							public void onMessage(ResponseOk item, final Messaging<Command> messaging) {
								StreamByteChunker byteChunker = new StreamByteChunker(eventloop, minChunkSize, maxChunkSize);
								producer.streamTo(byteChunker.getInput());
								messaging.write(byteChunker.getOutput(), new CompletionCallback() {
									@Override
									public void onComplete() {

									}

									@Override
									public void onException(final Exception e) {
										messaging.shutdown();
										commit(server, fileName, false, callback);
									}
								});
							}
						})
						.addHandler(ResponseAcknowledge.class, new MessagingHandler<ResponseAcknowledge, Command>() {
							@Override
							public void onMessage(ResponseAcknowledge item, Messaging<Command> messaging) {
								messaging.shutdown();
								commit(server, fileName, true, callback);
							}
						})
						.addHandler(ResponseError.class, new MessagingHandler<ResponseError, Command>() {
							@Override
							public void onMessage(ResponseError item, Messaging<Command> messaging) {
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

	private ConnectCallback defineDownload(final String fileName, final StreamConsumer<ByteBuf> consumer,
	                                       final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<Command>() {
							@Override
							public void onStart(Messaging<Command> messaging) {
								CommandDownload commandDownload = new CommandDownload(fileName);
								messaging.sendMessage(commandDownload);
							}
						})
						.addHandler(ResponseOk.class, new MessagingHandler<ResponseOk, Command>() {
							@Override
							public void onMessage(ResponseOk item, Messaging<Command> messaging) {
								StreamProducer<ByteBuf> producer = messaging.read();
								producer.streamTo(consumer);
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(ResponseError.class, new MessagingHandler<ResponseError, Command>() {
							@Override
							public void onMessage(ResponseError item, Messaging<Command> messaging) {
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

	private ConnectCallback defineDelete(final String fileName, final CompletionCallback callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<Command>() {
							@Override
							public void onStart(Messaging<Command> messaging) {
								Command commandDelete = new CommandDelete(fileName);
								messaging.sendMessage(commandDelete);
							}
						})
						.addHandler(ResponseOk.class, new MessagingHandler<ResponseOk, Command>() {
							@Override
							public void onMessage(ResponseOk item, Messaging<Command> messaging) {
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(ResponseError.class, new MessagingHandler<ResponseError, Command>() {
							@Override
							public void onMessage(ResponseError item, Messaging<Command> messaging) {
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

	private ConnectCallback defineList(final ResultCallback<Set<String>> callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<Command>() {
							@Override
							public void onStart(Messaging<Command> messaging) {
								Command commandList = new CommandList();
								messaging.sendMessage(commandList);
							}
						})
						.addHandler(ResponseListFiles.class, new MessagingHandler<ResponseListFiles, Command>() {
							@Override
							public void onMessage(ResponseListFiles item, Messaging<Command> messaging) {
								messaging.shutdown();
								callback.onResult(item.files);
							}
						})
						.addHandler(ResponseError.class, new MessagingHandler<ResponseError, Command>() {
							@Override
							public void onMessage(ResponseError item, Messaging<Command> messaging) {
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

	private ConnectCallback defineAlive(final ResultCallback<Set<ServerInfo>> callback) {
		return new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<Command>() {
							@Override
							public void onStart(Messaging<Command> messaging) {
								Command checkAlive = new CommandAlive();
								messaging.sendMessage(checkAlive);
							}
						})
						.addHandler(ResponseListServers.class, new MessagingHandler<ResponseListServers, Command>() {
							@Override
							public void onMessage(ResponseListServers item, Messaging<Command> messaging) {
								messaging.shutdown();
								callback.onResult(item.servers);
							}
						})
						.addHandler(ResponseError.class, new MessagingHandler<ResponseError, Command>() {
							@Override
							public void onMessage(ResponseError item, Messaging<Command> messaging) {
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
						.addStarter(new MessagingStarter<Command>() {
							@Override
							public void onStart(Messaging<Command> messaging) {
								Command offerCommand = new CommandOffer(forDeletion, forUpload);
								messaging.sendMessage(offerCommand);
							}
						})
						.addHandler(ResponseListFiles.class, new MessagingHandler<ResponseListFiles, Command>() {
							@Override
							public void onMessage(ResponseListFiles item, Messaging<Command> messaging) {
								messaging.shutdown();
								callback.onResult(item.files);
							}
						})
						.addHandler(ResponseError.class, new MessagingHandler<ResponseError, Command>() {
							@Override
							public void onMessage(ResponseError item, Messaging<Command> messaging) {
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

	private void commit(ServerInfo server, final String fileName, final boolean success,
	                    final CompletionCallback callback) {
		connect(server.getAddress(), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<Command>() {
							@Override
							public void onStart(Messaging<Command> messaging) {
								CommandCommit commandCommit = new CommandCommit(fileName, success);
								messaging.sendMessage(commandCommit);
							}
						})
						.addHandler(ResponseOk.class, new MessagingHandler<ResponseOk, Command>() {
							@Override
							public void onMessage(ResponseOk item, Messaging<Command> messaging) {
								messaging.shutdown();
								if (success) {
									callback.onComplete();
								} else {
									callback.onException(new Exception("Can't send file"));
								}
							}
						})
						.addHandler(ResponseError.class, new MessagingHandler<ResponseError, Command>() {
							@Override
							public void onMessage(ResponseError item, Messaging<Command> messaging) {
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

	private StreamMessagingConnection<Response, Command> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, ResponseSerializer.GSON, Response.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, CommandSerializer.GSON, Command.class,
						serializerBufferSize, serializerMaxMessageSize, serializerFlushDelayMillis));
	}
}