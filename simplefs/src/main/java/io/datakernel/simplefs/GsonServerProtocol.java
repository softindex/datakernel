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
import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.nio.channels.SocketChannel;
import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

final class GsonServerProtocol extends AbstractNioServer<GsonServerProtocol> {
	public static final class Builder {
		private final NioEventloop eventloop;
		private int deserializerBufferSize = DEFAULT_DESERIALIZER_BUFFER_SIZE;
		private int serializerBufferSize = DEFAULT_SERIALIZER_BUFFER_SIZE;
		private int serializerMaxMessageSize = DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE;
		private int serializerFlushDelayMillis = DEFAULT_SERIALIZER_FLUSH_DELAY_MS;

		private Builder(NioEventloop eventloop) {
			this.eventloop = eventloop;
		}

		public Builder setDeserializerBufferSize(int deserializerBufferSize) {
			this.deserializerBufferSize = deserializerBufferSize;
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

		public GsonServerProtocol build() {
			return new GsonServerProtocol(eventloop, deserializerBufferSize, serializerBufferSize,
					serializerMaxMessageSize, serializerFlushDelayMillis);
		}
	}

	public static final int DEFAULT_DESERIALIZER_BUFFER_SIZE = 10;
	public static final int DEFAULT_SERIALIZER_BUFFER_SIZE = 256 * 1024;
	public static final int DEFAULT_SERIALIZER_MAX_MESSAGE_SIZE = 256 * (1 << 20);
	public static final int DEFAULT_SERIALIZER_FLUSH_DELAY_MS = 0;

	private SimpleFsServer server;
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

	void wireServer(SimpleFsServer server) {
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
				.addHandler(FsCommand.List.class, defineListHandler());
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
				long size = server.size(item.filePath);
				if (size < 0) {
					messaging.sendMessage(new FsResponse.Error("File not found"));
					messaging.shutdown();
				} else {
					messaging.sendMessage(new FsResponse.Ready(size));
					messaging.write(server.download(item.filePath), ignoreCompletionCallback());
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
}