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

package io.datakernel.hashfs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.nio.channels.SocketChannel;
import java.util.Set;

final class GsonServerProtocol extends ServerProtocol {
	public static class Builder {
		private final NioEventloop eventloop;
		private int deserializerBufferSize = DEFAULT_DESERIALIZER_BUFFER_SIZE;
		private int serializerBufferSize = DEFAULT_SERIALIZER_BUFFER_SIZE;
		private int serializerMaxMessageSize = DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE;
		private int serializerFlushDelayMillis = DEFAULT_SERIALIZER_FLUSH_DELAY_MS;

		private Builder(NioEventloop eventloop) {
			this.eventloop = eventloop;
		}

		public void setDeserializerBufferSize(int deserializerBufferSize) {
			this.deserializerBufferSize = deserializerBufferSize;
		}

		public void setSerializerBufferSize(int serializerBufferSize) {
			this.serializerBufferSize = serializerBufferSize;
		}

		public void setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			this.serializerMaxMessageSize = serializerMaxMessageSize;
		}

		public void setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			this.serializerFlushDelayMillis = serializerFlushDelayMillis;
		}

		public GsonServerProtocol build() {
			return new GsonServerProtocol(eventloop, deserializerBufferSize, serializerBufferSize,
					serializerMaxMessageSize, serializerFlushDelayMillis);
		}
	}

	public static final int DEFAULT_DESERIALIZER_BUFFER_SIZE = 10;
	public static final int DEFAULT_SERIALIZER_BUFFER_SIZE = 256 * 1024;
	public static final int DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE = 256 * (1 << 20);
	public static final int DEFAULT_SERIALIZER_FLUSH_DELAY_MS = 0;

	private HashFsServer server;
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

	public static Builder buildInstance(NioEventloop eventloop) {
		return new Builder(eventloop);
	}

	@Override
	void wire(HashFsServer server) {
		this.server = server;
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, FsCommand.getGSON(), FsCommand.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, FsResponse.getGSON(), FsResponse.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis))
				.addHandler(FsCommand.Upload.class, defineUploadHandler())
				.addHandler(FsCommand.Commit.class, defineCommitHandler())
				.addHandler(FsCommand.Download.class, defineDownloadHandler())
				.addHandler(FsCommand.Delete.class, defineDeleteHandler())
				.addHandler(FsCommand.List.class, defineListHandler())
				.addHandler(FsCommand.Alive.class, defineAliveHandler())
				.addHandler(FsCommand.Offer.class, defineOfferHandler());
	}

	protected MessagingHandler<FsCommand.Upload, FsResponse> defineUploadHandler() {
		return new MessagingHandler<FsCommand.Upload, FsResponse>() {
			@Override
			public void onMessage(FsCommand.Upload item, final Messaging<FsResponse> messaging) {
				messaging.sendMessage(new FsResponse.Ok());
				server.upload(item.filePath, messaging.read(), new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new FsResponse.Acknowledge());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new FsResponse.Error(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	protected MessagingHandler<FsCommand.Commit, FsResponse> defineCommitHandler() {
		return new MessagingHandler<FsCommand.Commit, FsResponse>() {
			@Override
			public void onMessage(FsCommand.Commit item, final Messaging<FsResponse> messaging) {
				server.commit(item.filePath, item.isOk, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new FsResponse.Ok());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new FsResponse.Error(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	protected MessagingHandler<FsCommand.Download, FsResponse> defineDownloadHandler() {
		return new MessagingHandler<FsCommand.Download, FsResponse>() {
			@Override
			public void onMessage(FsCommand.Download item, final Messaging<FsResponse> messaging) {
				final long size = server.fileSize(item.filePath);
				if (size < 0) {
					messaging.sendMessage(new FsResponse.Error("File not found"));
					messaging.shutdown();
				} else {
					final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
					messaging.sendMessage(new FsResponse.Ready(size));
					server.download(item.filePath, forwarder.getInput(), new ResultCallback<CompletionCallback>() {
						@Override
						public void onResult(final CompletionCallback callback) {
							messaging.write(forwarder.getOutput(), callback);
							messaging.shutdownWriter();
						}

						@Override
						public void onException(Exception e) {
							messaging.sendMessage(new FsResponse.Error(e.getMessage()));
						}
					});
					messaging.shutdownWriter();
				}

			}
		};
	}

	protected MessagingHandler<FsCommand.List, FsResponse> defineListHandler() {
		return new MessagingHandler<FsCommand.List, FsResponse>() {
			@Override
			public void onMessage(FsCommand.List item, final Messaging<FsResponse> messaging) {
				server.list(new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new FsResponse.ListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new FsResponse.Error(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	protected MessagingHandler<FsCommand.Delete, FsResponse> defineDeleteHandler() {
		return new MessagingHandler<FsCommand.Delete, FsResponse>() {
			@Override
			public void onMessage(FsCommand.Delete item, final Messaging<FsResponse> messaging) {
				server.delete(item.filePath, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new FsResponse.Ok());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new FsResponse.Error(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<FsCommand.Alive, FsResponse> defineAliveHandler() {
		return new MessagingHandler<FsCommand.Alive, FsResponse>() {
			@Override
			public void onMessage(FsCommand.Alive item, final Messaging<FsResponse> messaging) {
				server.showAlive(new ResultCallback<Set<ServerInfo>>() {
					@Override
					public void onResult(Set<ServerInfo> result) {
						messaging.sendMessage(new FsResponse.ListServers(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new FsResponse.Error(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<FsCommand.Offer, FsResponse> defineOfferHandler() {
		return new MessagingHandler<FsCommand.Offer, FsResponse>() {
			@Override
			public void onMessage(FsCommand.Offer item, final Messaging<FsResponse> messaging) {
				server.checkOffer(item.forUpload, item.forDeletion, new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new FsResponse.ListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new FsResponse.Error(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}
}