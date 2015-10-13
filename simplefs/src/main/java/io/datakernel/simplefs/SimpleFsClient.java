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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SimpleFsClient implements SimpleFs {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsClient.class);

	private final InetSocketAddress address;
	private final NioEventloop eventloop;
	private final int bufferSize;

	public SimpleFsClient(NioEventloop eventloop, int bufferSize, InetSocketAddress address) {
		this.eventloop = eventloop;
		this.bufferSize = bufferSize;
		this.address = address;
	}

	public SimpleFsClient(NioEventloop eventloop, InetSocketAddress address) {
		this(eventloop, 128 * 1024, address);
	}

	@Override
	public void upload(final String fileName, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<SimpleFsCommand>() {
							@Override
							public void onStart(Messaging<SimpleFsCommand> messaging) {
								logger.info("Request for file {} upload sent", fileName);
								messaging.sendMessage(new SimpleFsCommandUpload(fileName));
							}
						})
						.addHandler(SimpleFsResponseOperationOk.class, new MessagingHandler<SimpleFsResponseOperationOk, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseOperationOk item, final Messaging<SimpleFsCommand> messaging) {
								logger.info("Uploading file {}", fileName);
								StreamByteChunker streamByteChunker = new StreamByteChunker(eventloop, bufferSize / 2, bufferSize);
								producer.streamTo(streamByteChunker);
								messaging.write(streamByteChunker, new CompletionCallback() {
									@Override
									public void onComplete() {

									}

									@Override
									public void onException(Exception exception) {

									}
								});
							}
						})
						.addHandler(SimpleFsResponseAcknowledge.class, new MessagingHandler<SimpleFsResponseAcknowledge, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseAcknowledge item, Messaging<SimpleFsCommand> messaging) {
								logger.info("Received acknowledgement for {}", fileName);
								messaging.shutdown();
								commit(fileName, callback);
							}
						})
						.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> messaging) {
								Exception e = new Exception(item.exceptionMsg);
								logger.error("Can't upload file {}", fileName, e);
								messaging.shutdown();
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't connect", e);
				callback.onException(e);
			}
		});
	}

	@Override
	public void download(final String fileName, final StreamConsumer<ByteBuf> consumer) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				final SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<SimpleFsCommand>() {
							@Override
							public void onStart(Messaging<SimpleFsCommand> messaging) {
								logger.info("Request for file {} download sent", fileName);
								SimpleFsCommandDownload commandDownload = new SimpleFsCommandDownload(fileName);
								messaging.sendMessage(commandDownload);
							}
						})
						.addHandler(SimpleFsResponseOperationOk.class, new MessagingHandler<SimpleFsResponseOperationOk, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseOperationOk item, Messaging<SimpleFsCommand> messaging) {
								logger.info("Downloading file {}", fileName);
								StreamProducer<ByteBuf> producer = messaging.binarySocketReader();
								producer.streamTo(consumer);
								messaging.shutdownWriter();
							}
						})
						.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> messaging) {
								Exception e = new Exception(item.exceptionMsg);
								logger.error("Can't download {}", fileName, e);
								StreamProducers.<ByteBuf>closingWithError(eventloop, e)
										.streamTo(consumer);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't connect", e);
				StreamProducers.<ByteBuf>closingWithError(eventloop, e)
						.streamTo(consumer);
			}
		});
	}

	@Override
	public void listFiles(final ResultCallback<List<String>> callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<SimpleFsCommand>() {
							@Override
							public void onStart(Messaging<SimpleFsCommand> messaging) {
								logger.info("Request to list files sent");
								SimpleFsCommand commandList = new SimpleFsCommandList();
								messaging.sendMessage(commandList);
							}
						})
						.addHandler(SimpleFsResponseFileList.class, new MessagingHandler<SimpleFsResponseFileList, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseFileList item, Messaging<SimpleFsCommand> messaging) {
								logger.info("Received list of files");
								messaging.shutdown();
								callback.onResult(item.fileList);
							}
						})
						.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> messaging) {
								Exception e = new Exception(item.exceptionMsg);
								messaging.shutdown();
								logger.error("Can't list files", e);
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't connect", e);
				callback.onException(e);
			}
		});
	}

	@Override
	public void deleteFile(final String fileName, final CompletionCallback callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				final SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<SimpleFsCommand>() {
							@Override
							public void onStart(Messaging<SimpleFsCommand> messaging) {
								logger.info("Request to delete file {} sent", fileName);
								SimpleFsCommand commandDelete = new SimpleFsCommandDelete(fileName);
								messaging.sendMessage(commandDelete);
							}
						})
						.addHandler(SimpleFsResponseOperationOk.class, new MessagingHandler<SimpleFsResponseOperationOk, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseOperationOk item, Messaging<SimpleFsCommand> messaging) {
								logger.info("File {} deleted", fileName);
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> messaging) {
								Exception e = new Exception(item.exceptionMsg);
								logger.error("Can't delete {}", fileName, e);
								messaging.shutdown();
								callback.onException(e);
							}
						});
				connection.register();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't connect", e);
				callback.onException(e);
			}
		});
	}

	private StreamMessagingConnection<SimpleFsResponse, SimpleFsCommand> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 10),
				new StreamGsonSerializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 256 * 1024, 256 * (1 << 20), 0));
	}

	private void commit(final String fileName, final CompletionCallback callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<SimpleFsCommand>() {
							@Override
							public void onStart(Messaging<SimpleFsCommand> messaging) {
								SimpleFsCommandCommit commandCommit = new SimpleFsCommandCommit(fileName);
								messaging.sendMessage(commandCommit);
							}
						})
						.addHandler(SimpleFsResponseOperationOk.class, new MessagingHandler<SimpleFsResponseOperationOk, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseOperationOk item, Messaging<SimpleFsCommand> messaging) {
								logger.trace("File {} approved for commit", fileName);
								messaging.shutdown();
								callback.onComplete();
							}
						})
						.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
							@Override
							public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> messaging) {
								logger.trace("Can't commit {}: {}", fileName, item.exceptionMsg);
								messaging.shutdown();
								callback.onException(new Exception(item.exceptionMsg));
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
}
