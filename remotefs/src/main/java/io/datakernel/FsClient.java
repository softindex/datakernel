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

package io.datakernel;

import com.google.gson.Gson;
import io.datakernel.FsCommands.FsCommand;
import io.datakernel.FsResponses.FsResponse;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
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

public abstract class FsClient {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop;

	private int minChunkSize = 64 * (1 << 10);
	private int maxChunkSize = 128 * (1 << 10);
	private int connectTimeout = 0;
	private SocketSettings socketSettings = SocketSettings.defaultSocketSettings();

	private int serializerBufferSize = 256 * (1 << 10);
	private int serializerMaxMessageSize = 256 * (1 << 20);
	private int serializerFlushDelayMillis = 0;
	private int deserializerBufferSize = 256 * (1 << 10);

	// creators & builders
	public FsClient(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	public FsClient setChunkSizeRange(int min, int max) {
		check(serializerMaxMessageSize > 0, "Min chunk size should be positive");
		check(maxChunkSize > minChunkSize, "Max should be bigger then min");
		this.minChunkSize = min;
		this.maxChunkSize = max;
		return this;
	}

	public FsClient setConnectTimeout(int connectTimeout) {
		check(connectTimeout >= 0, "Connect timeout should be >= 0");
		this.connectTimeout = connectTimeout;
		return this;
	}

	public FsClient setSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return this;
	}

	public FsClient setSerializerBufferSize(int serializerBufferSize) {
		check(serializerBufferSize > 0, "Serializer buffer size should be positive");
		this.serializerBufferSize = serializerBufferSize;
		return this;
	}

	public FsClient setSerializerMaxMessageSize(int serializerMaxMessageSize) {
		check(serializerMaxMessageSize > 0, "Serializer max message size should be positive");
		this.serializerMaxMessageSize = serializerMaxMessageSize;
		return this;
	}

	public FsClient setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
		check(serializerFlushDelayMillis >= 0, "Serializer flush delay should be >= 0");
		this.serializerFlushDelayMillis = serializerFlushDelayMillis;
		return this;
	}

	public FsClient setDeserializerBufferSize(int deserializerBufferSize) {
		check(deserializerBufferSize > 0, "Deserializer buffer size should be positive");
		this.deserializerBufferSize = deserializerBufferSize;
		return this;
	}

	// api
	public abstract void upload(String destinationFileName, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	public abstract void download(String sourceFileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback);

	public abstract void list(ResultCallback<List<String>> callback);

	public abstract void delete(String fileName, CompletionCallback callback);

	// transport code
	protected final void doUpload(InetSocketAddress address, String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		connect(address, new UploadConnectCallback(fileName, producer, callback));
	}

	protected final void doDownload(InetSocketAddress address, String fileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback) {
		connect(address, new DownloadConnectCallback(callback, fileName, startPosition));
	}

	protected final void doDelete(InetSocketAddress address, String fileName, CompletionCallback callback) {
		connect(address, new DeleteConnectCallback(callback, fileName));
	}

	protected final void doList(InetSocketAddress address, ResultCallback<List<String>> callback) {
		connect(address, new ListConnectCallback(callback));
	}

	// establishing connection
	protected final StreamMessagingConnection<FsResponse, FsCommand> createConnection(SocketChannel channel) {
		return new StreamMessagingConnection<>(eventloop, channel,
				new StreamGsonDeserializer<>(eventloop, getResponseGson(), FsResponse.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, getCommandGSON(), FsCommand.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis));
	}

	protected Gson getCommandGSON() {return FsCommands.commandGSON;}

	protected Gson getResponseGson() {return FsResponses.responseGson;}

	protected final void connect(SocketAddress address, ConnectCallback callback) {
		eventloop.connect(address, socketSettings, connectTimeout, callback);
	}

	private final class UploadConnectCallback implements ConnectCallback {
		private final CompletionCallback callback;
		private final String file;
		private final StreamProducer<ByteBuf> producer;

		UploadConnectCallback(String file, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
			this.callback = callback;
			this.file = file;
			this.producer = producer;
		}

		@Override
		public void onConnect(SocketChannel channel) {
			SocketConnection connection = createConnection(channel)
					.addStarter(new MessagingStarter<FsCommand>() {
						@Override
						public void onStart(Messaging<FsCommand> messaging) {
							logger.trace("send command to upload {}", file);
							messaging.sendMessage(new FsCommands.Upload(file));
						}
					})
					.addHandler(FsResponses.Ok.class, new MessagingHandler<FsResponses.Ok, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Ok item, final Messaging<FsCommand> messaging) {
							logger.trace("received ok for {}, start streaming", file);
							StreamByteChunker byteChunker = new StreamByteChunker(eventloop, minChunkSize, maxChunkSize);
							producer.streamTo(byteChunker.getInput());
							messaging.write(byteChunker.getOutput(), new CompletionCallback() {
								@Override
								public void onComplete() {
									logger.info("Finished streaming {}", file);
								}

								@Override
								public void onException(Exception e) {
									logger.info("Failed to stream {}", file);
									callback.onException(e);
								}
							});
						}
					})
					.addHandler(FsResponses.Acknowledge.class, new MessagingHandler<FsResponses.Acknowledge, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Acknowledge item, Messaging<FsCommand> messaging) {
							logger.trace("received acknowledge for {}", file);
							messaging.shutdown();
							callback.onComplete();
						}
					})
					.addHandler(FsResponses.Err.class, new MessagingHandler<FsResponses.Err, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Err item, Messaging<FsCommand> messaging) {
							logger.trace("failed to upload file {}", file);
							messaging.shutdown();
							callback.onException(new Exception(item.msg));
						}
					})
					.addReadExceptionHandler(new MessagingExceptionHandler() {
						@Override
						public void onException(Exception e) {
							logger.trace("caught exception while trying to upload {}", file);
							callback.onException(e);
						}
					});
			connection.register();
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private final class DeleteConnectCallback implements ConnectCallback {
		private final CompletionCallback callback;
		private final String fileName;

		DeleteConnectCallback(CompletionCallback callback, String fileName) {
			this.callback = callback;
			this.fileName = fileName;
		}

		@Override
		public void onConnect(SocketChannel channel) {
			SocketConnection connection = createConnection(channel)
					.addStarter(new MessagingStarter<FsCommand>() {
						@Override
						public void onStart(Messaging<FsCommand> messaging) {
							logger.trace("send command to delete {}", fileName);
							messaging.sendMessage(new FsCommands.Delete(fileName));
						}
					})
					.addHandler(FsResponses.Ok.class, new MessagingHandler<FsResponses.Ok, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Ok item, Messaging<FsCommand> messaging) {
							logger.trace("succeed to delete {}", fileName);
							messaging.shutdown();
							callback.onComplete();
						}
					})
					.addHandler(FsResponses.Err.class, new MessagingHandler<FsResponses.Err, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Err item, Messaging<FsCommand> messaging) {
							logger.trace("failed to delete {}: {}", fileName, item.msg);
							messaging.shutdown();
							callback.onException(new Exception(item.msg));
						}
					})
					.addReadExceptionHandler(new MessagingExceptionHandler() {
						@Override
						public void onException(Exception e) {
							logger.trace("caught exception while trying to delete {}", fileName);
							callback.onException(e);
						}
					});
			connection.register();
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private final class DownloadConnectCallback implements ConnectCallback {
		private final ResultCallback<StreamTransformerWithCounter> callback;
		private final String file;
		private final long startPosition;

		DownloadConnectCallback(ResultCallback<StreamTransformerWithCounter> callback, String file, long startPosition) {
			this.callback = callback;
			this.file = file;
			this.startPosition = startPosition;
		}

		@Override
		public void onConnect(SocketChannel channel) {
			SocketConnection connection = createConnection(channel)
					.addStarter(new MessagingStarter<FsCommand>() {
						@Override
						public void onStart(Messaging<FsCommand> messaging) {
							logger.trace("send command to download {}", file);
							messaging.sendMessage(new FsCommands.Download(file, startPosition));
						}
					})
					.addHandler(FsResponses.Ready.class, new MessagingHandler<FsResponses.Ready, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Ready item, Messaging<FsCommand> messaging) {
							logger.trace("received acknowledge for {} bytes ready", item.size);
							StreamTransformerWithCounter counter = new StreamTransformerWithCounter(eventloop, item.size - startPosition);
							messaging.read().streamTo(counter.getInput());
							callback.onResult(counter);
							messaging.shutdown();
						}
					})
					.addHandler(FsResponses.Err.class, new MessagingHandler<FsResponses.Err, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Err item, Messaging<FsCommand> messaging) {
							logger.trace("failed to download {}", item.msg);
							messaging.shutdown();
							Exception e = new Exception(item.msg);
							callback.onException(e);
						}
					})
					.addReadExceptionHandler(new MessagingExceptionHandler() {
						@Override
						public void onException(Exception e) {
							logger.trace("caught exception while trying to download {}", file);
							callback.onException(e);
						}
					});
			connection.register();
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private final class ListConnectCallback implements ConnectCallback {
		private final ResultCallback<List<String>> callback;

		ListConnectCallback(ResultCallback<List<String>> callback) {this.callback = callback;}

		@Override
		public void onConnect(SocketChannel channel) {
			SocketConnection connection = createConnection(channel)
					.addStarter(new MessagingStarter<FsCommand>() {
						@Override
						public void onStart(Messaging<FsCommand> messaging) {
							logger.trace("send command to list files");
							messaging.sendMessage(new FsCommands.ListFiles());
						}
					})
					.addHandler(FsResponses.ListOfFiles.class, new MessagingHandler<FsResponses.ListOfFiles, FsCommand>() {
						@Override
						public void onMessage(FsResponses.ListOfFiles item, Messaging<FsCommand> messaging) {
							logger.trace("received list of {} files", item.files.size());
							messaging.shutdown();
							callback.onResult(item.files);
						}
					})
					.addHandler(FsResponses.Err.class, new MessagingHandler<FsResponses.Err, FsCommand>() {
						@Override
						public void onMessage(FsResponses.Err item, Messaging<FsCommand> messaging) {
							logger.trace("failed to list files {}");
							messaging.shutdown();
							callback.onException(new Exception(item.msg));
						}
					})
					.addReadExceptionHandler(new MessagingExceptionHandler() {
						@Override
						public void onException(Exception e) {
							logger.trace("caught exception while trying to list files");
							callback.onException(e);
						}
					});
			connection.register();
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}
}