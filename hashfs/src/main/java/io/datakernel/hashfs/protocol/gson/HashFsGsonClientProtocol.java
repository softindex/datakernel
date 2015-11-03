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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ForwardingConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.FileMetadata;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.hashfs.protocol.HashFsClientProtocol;
import io.datakernel.hashfs.protocol.gson.commands.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.*;

public class HashFsGsonClientProtocol implements HashFsClientProtocol {
	private static final Logger logger = LoggerFactory.getLogger(HashFsGsonClientProtocol.class);

	private final NioEventloop eventloop;
	private final Map<Integer, ServerInfo> idServerInfo = new HashMap<>();

	public HashFsGsonClientProtocol(NioEventloop eventloop, List<ServerInfo> allServers) {
		this.eventloop = eventloop;

		for (ServerInfo serverInfo : allServers) {
			idServerInfo.put(serverInfo.serverId, serverInfo);
		}
	}

	private StreamMessagingConnection<HashFsResponse, HashFsCommand> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsResponseSerialization.GSON, HashFsResponse.class, 256 * 1024),
				new StreamGsonSerializer<>(eventloop, HashFsCommandSerialization.GSON, HashFsCommand.class, 256 * 1024, 256 * (1 << 20), 0));
	}

	@Override
	public void upload(final ServerInfo server, final String destinationName, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		connect(server, new ForwardingConnectCallback(callback) {

					private final WriteTransformerAck<ByteBuf> ackConsumer = new WriteTransformerAck<>(eventloop);

					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<HashFsCommand>() {
									@Override
									public void onStart(Messaging<HashFsCommand> messaging) {
										logger.info("Client send upload command file {} to {}", destinationName, server.serverId);
										HashFsCommand commandUpload = new HashFsCommandUpload(destinationName);
										messaging.sendMessage(commandUpload);
									}
								})
								.addHandler(HashFsResponseOperationOk.class, new MessagingHandler<HashFsResponseOperationOk, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseOperationOk item, Messaging<HashFsCommand> messaging) {
//										ackConsumer.streamTo(messaging.binarySocketWriter());
										callback.onResult(ackConsumer);
									}
								})
								.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
										logger.warn("Receive error from server {}: {}", server.serverId, item.exceptionName);
										messaging.shutdown();
										callback.onException(new Exception(item.exceptionName));
										// TODO (eberezhanskyi): will consumer be notified on errors during transfer?
									}
								})
								.addHandler(HashFsResponseFileUploaded.class, new MessagingHandler<HashFsResponseFileUploaded, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseFileUploaded item, Messaging<HashFsCommand> messaging) {
										messaging.shutdown();
										logger.info("Client receive file {} uploaded", destinationName);
										ackConsumer.receiveAck();
										// TODO (eberezhanskyi): what if connection is broken?.. and no ack is received at all?..
										// TODO timeouts
									}
								});
						connection.register();
					}
				}
		);
	}

	private void connect(final ServerInfo server, final ForwardingConnectCallback connectCallback) {
		eventloop.connect(server.commandListenAddress, new SocketSettings(), connectCallback);
	}

	private ServerInfo getServerById(int serverId) {
		return idServerInfo.get(serverId);
	}

	@Override
	public void replicas(ServerInfo server, final ResultCallback<Map<String, Set<Integer>>> callback) {
		connect(server, new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								messaging.sendMessage(new HashFsCommandFileReplicas());
							}
						})
						.addHandler(HashFsResponseFileReplicas.class, new MessagingHandler<HashFsResponseFileReplicas, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseFileReplicas item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onResult(item.replicasMap);
							}
						});
				connection.register();
			}
		});

	}

	@Override
	public void list(final ServerInfo server, final ResultCallback<Set<String>> callback) {
		connect(server, new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								messaging.sendMessage(new HashFsCommandList());
							}
						})
						.addHandler(HashFsResponseFiles.class, new MessagingHandler<HashFsResponseFiles, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseFiles item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onResult(item.files);
							}
						});
				connection.register();
			}
		});
	}

	@Override
	public void listAbsent(ServerInfo server, final Map<String, FileMetadata.FileBaseInfo> interestFiles,
	                       final ResultCallback<Set<String>> callback) {
		connect(server, new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								messaging.sendMessage(new HashFsCommandAbsentFiles(interestFiles));
							}
						})
						.addHandler(HashFsResponseFiles.class, new MessagingHandler<HashFsResponseFiles, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseFiles item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								callback.onResult(item.files);
							}
						});
				connection.register();
			}
		});

	}

	@Override
	public void getAliveServers(final ServerInfo server, final ResultCallback<List<ServerInfo>> callback) {
		connect(server, new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								logger.info("Send request for alive servers to {}", server.serverId);
								messaging.sendMessage(new HashFsCommandAliveServers());
								// TODO timeout for answer
							}
						})
						.addHandler(HashFsResponseAliveServers.class, new MessagingHandler<HashFsResponseAliveServers, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseAliveServers item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								logger.info("Receive response for alive servers count {}", item.aliveServers.size());
								List<ServerInfo> aliveServers = new ArrayList<>();
								for (Integer serverId : item.aliveServers) {
									aliveServers.add(getServerById(serverId));
								}
								callback.onResult(aliveServers);
								messaging.shutdown();
							}
						});
				connection.register();
			}
		});
	}

	@Override
	public void download(final ServerInfo server, final String fileName, final ResultCallback<StreamProducer<ByteBuf>> callback) {

		connect(server, new ForwardingConnectCallback(callback) {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						StreamMessagingConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<HashFsCommand>() {
									@Override
									public void onStart(Messaging<HashFsCommand> messaging) {
										HashFsCommandDownload commandDownload = new HashFsCommandDownload(fileName);
										messaging.sendMessage(commandDownload);
									}
								})
								.addHandler(HashFsResponseOperationOk.class, new MessagingHandler<HashFsResponseOperationOk, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseOperationOk item, Messaging<HashFsCommand> messaging) {
										messaging.shutdownWriter();
										callback.onResult(messaging.read());
									}
								})
								.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
										messaging.shutdown();
										callback.onException(new Exception(item.exceptionName));
									}
								});
						connection.register();
					}

				}
		);
	}

	@Override
	public void delete(ServerInfo server, final String fileName, final ResultCallback<Boolean> callback) {
		connect(server, new ForwardingConnectCallback(callback) {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<HashFsCommand>() {
									@Override
									public void onStart(Messaging<HashFsCommand> messaging) {
										HashFsCommand deleteCommand = new HashFsCommandDelete(fileName);
										messaging.sendMessage(deleteCommand);
									}
								})
								.addHandler(HashFsResponseDeletedByUser.class, new MessagingHandler<HashFsResponseDeletedByUser, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseDeletedByUser item, Messaging<HashFsCommand> messaging) {
										messaging.shutdown();
										callback.onResult(true);
									}
								})
								.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
										messaging.shutdown();
										callback.onException(new Exception(item.exceptionName));
									}
								});
						connection.register();
					}
				}
		);

	}

}
