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

package io.datakernel.hashfs.protocol;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.FsServer;
import io.datakernel.hashfs.RfsConfig;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.hashfs.ServerProtocol;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;

public final class GsonServerProtocol extends ServerProtocol {
	private FsServer server;
	private final int deserializerBufferSize;
	private final int serializerBufferSize;
	private final int serializerMaxMessageSize;
	private final int serializerFlushDelayMillis;

	private GsonServerProtocol(NioEventloop eventloop, int deserializerBufferSize,
	                           int serializerBufferSize, int serializerMaxMessageSize, int serializerFlushDelayMillis) {
		super(eventloop);
		this.deserializerBufferSize = deserializerBufferSize;
		this.serializerBufferSize = serializerBufferSize;
		this.serializerMaxMessageSize = serializerMaxMessageSize;
		this.serializerFlushDelayMillis = serializerFlushDelayMillis;
	}

	public static GsonServerProtocol createInstance(NioEventloop eventloop, List<InetSocketAddress> addresses, RfsConfig config) {
		GsonServerProtocol protocol = new GsonServerProtocol(eventloop,
				config.getDeserializerBufferSize(),
				config.getSerializerBufferSize(),
				config.getSerializerMaxMessageSize(),
				config.getSerializerFlushDelayMillis());
		protocol.setListenAddresses(addresses);
		return protocol;
	}

	@Override
	public void wireServer(FsServer server) {
		this.server = server;
	}

	@Override
	public void start(CompletionCallback callback) {
		try {
			self().listen();
			callback.onComplete();
		} catch (IOException e) {
			callback.onException(e);
		}
	}

	@Override
	public void stop(CompletionCallback callback) {
		self().close();
		callback.onComplete();
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, CommandSerializer.GSON, Command.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, ResponseSerializer.GSON, Response.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis))
				.addHandler(CommandUpload.class, defineUploadHandler())
				.addHandler(CommandCommit.class, defineCommitHandler())
				.addHandler(CommandDownload.class, defineDownloadHandler())
				.addHandler(CommandDelete.class, defineDeleteHandler())
				.addHandler(CommandList.class, defineListHandler())
				.addHandler(CommandAlive.class, defineAliveHandler())
				.addHandler(CommandOffer.class, defineOfferHandler());
	}

	private MessagingHandler<CommandUpload, Response> defineUploadHandler() {
		return new MessagingHandler<CommandUpload, Response>() {
			@Override
			public void onMessage(CommandUpload item, final Messaging<Response> messaging) {
				messaging.sendMessage(new ResponseOk());
				server.upload(item.filePath, messaging.read(), new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new ResponseAcknowledge());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new ResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<CommandCommit, Response> defineCommitHandler() {
		return new MessagingHandler<CommandCommit, Response>() {
			@Override
			public void onMessage(CommandCommit item, final Messaging<Response> messaging) {
				server.commit(item.filePath, item.isOk, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new ResponseOk());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new ResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<CommandDownload, Response> defineDownloadHandler() {
		return new MessagingHandler<CommandDownload, Response>() {
			@Override
			public void onMessage(CommandDownload item, final Messaging<Response> messaging) {
				final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
				server.download(item.filePath, forwarder.getInput(), new ResultCallback<CompletionCallback>() {
					@Override
					public void onResult(final CompletionCallback callback) {
						messaging.sendMessage(new ResponseOk());
						messaging.write(forwarder.getOutput(), new CompletionCallback() {
							@Override
							public void onComplete() {
								callback.onComplete();
							}

							@Override
							public void onException(Exception e) {
								messaging.sendMessage(new ResponseError(e.getMessage()));
								callback.onException(e);
							}
						});
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new ResponseError(e.getMessage()));
					}
				});
			}
		};
	}

	private MessagingHandler<CommandList, Response> defineListHandler() {
		return new MessagingHandler<CommandList, Response>() {
			@Override
			public void onMessage(CommandList item, final Messaging<Response> messaging) {
				server.list(new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new ResponseListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new ResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<CommandDelete, Response> defineDeleteHandler() {
		return new MessagingHandler<CommandDelete, Response>() {
			@Override
			public void onMessage(CommandDelete item, final Messaging<Response> messaging) {
				server.delete(item.filePath, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new ResponseOk());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new ResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<CommandAlive, Response> defineAliveHandler() {
		return new MessagingHandler<CommandAlive, Response>() {
			@Override
			public void onMessage(CommandAlive item, final Messaging<Response> messaging) {
				server.showAlive(new ResultCallback<Set<ServerInfo>>() {
					@Override
					public void onResult(Set<ServerInfo> result) {
						messaging.sendMessage(new ResponseListServers(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new ResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<CommandOffer, Response> defineOfferHandler() {
		return new MessagingHandler<CommandOffer, Response>() {
			@Override
			public void onMessage(CommandOffer item, final Messaging<Response> messaging) {
				server.checkOffer(item.forUpload, item.forDeletion, new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new ResponseListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new ResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}
}






























