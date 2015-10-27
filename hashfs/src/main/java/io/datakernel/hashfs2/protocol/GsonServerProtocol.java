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

package io.datakernel.hashfs2.protocol;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs2.Server;
import io.datakernel.hashfs2.ServerInfo;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.nio.channels.SocketChannel;
import java.util.Set;

public class GsonServerProtocol extends AbstractNioServer<GsonServerProtocol> {
	private final Server server;

	public GsonServerProtocol(NioEventloop eventloop, Server server) {
		super(eventloop);
		this.server = server;
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsCommandSerializer.GSON, HashFsCommand.class, 10),
				new StreamGsonSerializer<>(eventloop, HashFsResponseSerializer.GSON, HashFsResponse.class, 256 * 1024, 256 * (1 << 20), 0))
				.addHandler(HashFsCommandUpload.class, defineUploadHandler())
				.addHandler(HashFsCommandCommit.class, defineCommitHandler())
				.addHandler(HashFsCommandDownload.class, defineDownloadHandler())
				.addHandler(HashFsCommandDelete.class, defineDeleteHandler())
				.addHandler(HashFsCommandList.class, defineListHandler())
				.addHandler(HashFsCommandAlive.class, defineAliveHandler())
				.addHandler(HashFsCommandOffer.class, defineOfferHandler());
	}

	private MessagingHandler<HashFsCommandUpload, HashFsResponse> defineUploadHandler() {
		// FIXME (check) implement logic: req --> ok --> up --> ack (now: req+up --> ack)
		return new MessagingHandler<HashFsCommandUpload, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandUpload item, final Messaging<HashFsResponse> messaging) {
				messaging.sendMessage(new HashFsResponseOk());
				server.upload(item.filePath, messaging.read(), new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new HashFsResponseAcknowledge());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandCommit, HashFsResponse> defineCommitHandler() {
		return new MessagingHandler<HashFsCommandCommit, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandCommit item, final Messaging<HashFsResponse> messaging) {
				server.commit(item.filePath, item.isOk, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new HashFsResponseOk());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandDownload, HashFsResponse> defineDownloadHandler() {
		// FIXME CRUTCH
		return new MessagingHandler<HashFsCommandDownload, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandDownload item, final Messaging<HashFsResponse> messaging) {
				final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
				server.download(item.filePath, forwarder, new ResultCallback<CompletionCallback>() {
					@Override
					public void onResult(CompletionCallback callback) {
						messaging.write(forwarder, callback);
					}

					@Override
					public void onException(Exception ignored) {
						// ignored
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandList, HashFsResponse> defineListHandler() {
		return new MessagingHandler<HashFsCommandList, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandList item, final Messaging<HashFsResponse> messaging) {
				server.listFiles(new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new HashFsResponseListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandDelete, HashFsResponse> defineDeleteHandler() {
		return new MessagingHandler<HashFsCommandDelete, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandDelete item, final Messaging<HashFsResponse> messaging) {
				server.delete(item.filePath, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new HashFsResponseOk());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandAlive, HashFsResponse> defineAliveHandler() {
		return new MessagingHandler<HashFsCommandAlive, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandAlive item, final Messaging<HashFsResponse> messaging) {
				server.showAlive(new ResultCallback<Set<ServerInfo>>() {
					@Override
					public void onResult(Set<ServerInfo> result) {
						messaging.sendMessage(new HashFsResponseListServers(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandOffer, HashFsResponse> defineOfferHandler() {
		return new MessagingHandler<HashFsCommandOffer, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandOffer item, final Messaging<HashFsResponse> messaging) {
				server.checkOffer(item.forUpload, item.forDeletion, new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new HashFsResponseListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}
}






























