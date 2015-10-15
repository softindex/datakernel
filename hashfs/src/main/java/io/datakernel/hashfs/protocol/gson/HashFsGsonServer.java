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
import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.HashFsServer;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.hashfs.protocol.gson.commands.*;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashFsGsonServer extends AbstractNioServer<HashFsGsonServer> {
	private static final Logger logger = LoggerFactory.getLogger(HashFsGsonServer.class);

	private final NioEventloop eventloop;
	private final HashFsServer fileServer;
	private final ServerInfo myId;

	private HashFsGsonServer(NioEventloop eventloop, ServerInfo myId, HashFsServer fileServer) {
		super(eventloop);
		this.myId = myId;
		this.fileServer = fileServer;
		this.eventloop = eventloop;
	}

	public static HashFsGsonServer createServerTransport(NioEventloop eventloop, ServerInfo myId, HashFsServer fileServer) throws IOException {
		HashFsGsonServer serverTransport = new HashFsGsonServer(eventloop, myId, fileServer);
		serverTransport.setListenPort(myId.commandListenAddress.getPort());
		serverTransport.acceptOnce(false);
		serverTransport.listen();
		return serverTransport;
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {

		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsCommandSerialization.GSON, HashFsCommand.class, 256 * 1024),
				new StreamGsonSerializer<>(eventloop, HashFsResponseSerialization.GSON, HashFsResponse.class, 256 * 1024, 256 * (1 << 20), 1))
				.addHandler(HashFsCommandUpload.class, new MessagingHandler<HashFsCommandUpload, HashFsResponse>() {
					@Override
					public void onMessage(final HashFsCommandUpload item, final Messaging<HashFsResponse> messaging) {
						if (fileServer.canUpload(item.filename)) {
							messaging.sendMessage(new HashFsResponseOperationOk());
							StreamProducer<ByteBuf> producer = messaging.read();
							fileServer.onUpload(item.filename, producer, new CompletionCallback() {
								@Override
								public void onComplete() {
									logger.info("Send response file {} uploaded", item.filename);
									messaging.sendMessage(new HashFsResponseFileUploaded());
									messaging.shutdown();
								}

								@Override
								public void onException(Exception exception) {
									logger.info("Send can't upload file: {}", exception.getMessage());
									messaging.sendMessage(new HashFsResponseError(exception.getMessage()));
									messaging.shutdown();
								}
							});
						} else {
							messaging.sendMessage(new HashFsResponseError("File can't be uploaded."));
							messaging.shutdown();
						}

					}
				})
				.addHandler(HashFsCommandDownload.class, new MessagingHandler<HashFsCommandDownload, HashFsResponse>() {
					@Override
					public void onMessage(HashFsCommandDownload item, final Messaging<HashFsResponse> messaging) {
						fileServer.onDownload(item.filename, new ResultCallback<StreamProducer<ByteBuf>>() {
							@Override
							public void onResult(StreamProducer<ByteBuf> result) {
								messaging.sendMessage(new HashFsResponseOperationOk());
								result.streamTo(messaging.binarySocketWriter());
								messaging.shutdownReader();
							}

							@Override
							public void onException(Exception exception) {
								messaging.sendMessage(new HashFsResponseError(exception.getMessage()));
								messaging.shutdown();
							}
						});

					}
				})
				.addHandler(HashFsCommandDelete.class, new MessagingHandler<HashFsCommandDelete, HashFsResponse>() {
					@Override
					public void onMessage(final HashFsCommandDelete item, final Messaging<HashFsResponse> messaging) {
						fileServer.deleteFileByUser(item.filename, new CompletionCallback() {

							@Override
							public void onComplete() {
								messaging.sendMessage(new HashFsResponseDeletedByUser(item.filename));
								messaging.shutdown();
							}

							@Override
							public void onException(Exception exception) {
								messaging.sendMessage(new HashFsResponseError(exception.getMessage()));
								messaging.shutdown();
							}
						});
					}
				})
				.addHandler(HashFsCommandList.class, new MessagingHandler<HashFsCommandList, HashFsResponse>() {
					@Override
					public void onMessage(HashFsCommandList item, final Messaging<HashFsResponse> messaging) {
						fileServer.fileList(new ResultCallback<Set<String>>() {
							@Override
							public void onResult(Set<String> result) {
								messaging.sendMessage(new HashFsResponseFiles(result));
								messaging.shutdown();
							}

							@Override
							public void onException(Exception exception) {
								messaging.sendMessage(new HashFsResponseError(exception.getMessage()));
								messaging.shutdown();
							}
						});
					}
				})
				.addHandler(HashFsCommandFileReplicas.class, new MessagingHandler<HashFsCommandFileReplicas, HashFsResponse>() {
					@Override
					public void onMessage(HashFsCommandFileReplicas item, final Messaging<HashFsResponse> messaging) {
						fileServer.getFilesWithReplicas(item.interestingFiles, new ResultCallback<Map<String, Set<Integer>>>() {
							@Override
							public void onResult(Map<String, Set<Integer>> result) {
								messaging.sendMessage(new HashFsResponseFileReplicas(result));
								messaging.shutdown();
							}

							@Override
							public void onException(Exception exception) {
								messaging.sendMessage(new HashFsResponseError(exception.getMessage()));
								messaging.shutdown();
							}
						});
					}
				})
				.addHandler(HashFsCommandAliveServers.class, new MessagingHandler<HashFsCommandAliveServers, HashFsResponse>() {
					@Override
					public void onMessage(HashFsCommandAliveServers item, final Messaging<HashFsResponse> messaging) {
						logger.info("Receive query for alive servers {}", myId.serverId);
						fileServer.getAliveServers(new ResultCallback<List<ServerInfo>>() {
							@Override
							public void onResult(List<ServerInfo> result) {
								List<Integer> aliveServersId = new ArrayList<>();
								for (ServerInfo serverInfo : result) {
									aliveServersId.add(serverInfo.serverId);
								}
								messaging.sendMessage(new HashFsResponseAliveServers(aliveServersId));
							}

							@Override
							public void onException(Exception exception) {
								messaging.sendMessage(new HashFsResponseError(exception.getMessage()));
								messaging.shutdown();
							}
						});
					}
				})
				.addHandler(HashFsCommandAbsentFiles.class, new MessagingHandler<HashFsCommandAbsentFiles, HashFsResponse>() {
					@Override
					public void onMessage(HashFsCommandAbsentFiles item, Messaging<HashFsResponse> messaging) {
						messaging.sendMessage(new HashFsResponseFiles(fileServer.onAbsentFiles(item.fileBaseInfo)));
						messaging.shutdown();
					}
				});
	}

	public void closeAll() {
		super.close();
		fileServer.shutdown();
	}

}
