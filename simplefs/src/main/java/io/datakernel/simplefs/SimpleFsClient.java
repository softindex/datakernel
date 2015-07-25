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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.*;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;

import static io.datakernel.stream.processor.StreamLZ4Compressor.fastCompressor;

public class SimpleFsClient implements SimpleFs {
	public interface StreamLZ4CompressorFactory {
		StreamLZ4Compressor getInstance(Eventloop eventloop);
	}

	private final NioEventloop eventloop;
	private final int bufferSize;
	private final StreamLZ4CompressorFactory compressorFactory;

	public SimpleFsClient(NioEventloop eventloop, int bufferSize) {
		this(eventloop, bufferSize, new StreamLZ4CompressorFactory() {
			@Override
			public StreamLZ4Compressor getInstance(Eventloop eventloop) {
				return fastCompressor(eventloop);
			}
		});
	}

	public SimpleFsClient(NioEventloop eventloop, int bufferSize, StreamLZ4CompressorFactory compressorFactory) {
		this.eventloop = eventloop;
		this.bufferSize = bufferSize;
		this.compressorFactory = compressorFactory;
	}

	public SimpleFsClient(NioEventloop eventloop) {
		this(eventloop, 128 * 1024);
	}

	private StreamMessagingConnection<SimpleFsResponse, SimpleFsCommand> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 256 * 1024),
				new StreamGsonSerializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 256 * 1024, 256 * (1 << 20), 0));
	}

	@Override
	public void write(InetSocketAddress address, final String destinationFileName, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<SimpleFsCommand>() {
									@Override
									public void onStart(Messaging<SimpleFsCommand> messaging) {
										SimpleFsCommandUpload commandUpload = new SimpleFsCommandUpload(destinationFileName);
										messaging.sendMessage(commandUpload);
									}
								})
								.addHandler(SimpleFsResponseOperationOk.class, new MessagingHandler<SimpleFsResponseOperationOk, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseOperationOk item, Messaging<SimpleFsCommand> messaging) {
										StreamByteChunker streamByteChunkerBefore = new StreamByteChunker(eventloop, bufferSize / 2, bufferSize);
										StreamLZ4Compressor compressor = compressorFactory.getInstance(eventloop);
										StreamByteChunker streamByteChunkerAfter = new StreamByteChunker(eventloop, bufferSize / 2, bufferSize);

										streamByteChunkerBefore.streamTo(compressor);
										compressor.streamTo(streamByteChunkerAfter);
										streamByteChunkerAfter.streamTo(messaging.binarySocketWriter());

										callback.onResult(streamByteChunkerBefore);
										messaging.shutdownReader();
									}
								})
								.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> messaging) {
										messaging.shutdown();
										callback.onException(new Exception(item.exceptionName));
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception e) {
						callback.onException(e);
					}
				}
		);
	}

	@Override
	public void read(InetSocketAddress address, final String path, final ResultCallback<StreamProducer<ByteBuf>> callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<SimpleFsCommand>() {
									@Override
									public void onStart(Messaging<SimpleFsCommand> messaging) {
										SimpleFsCommandDownload commandDownload = new SimpleFsCommandDownload(path);
										messaging.sendMessage(commandDownload);
									}
								})
								.addHandler(SimpleFsResponseOperationOk.class, new MessagingHandler<SimpleFsResponseOperationOk, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseOperationOk item, Messaging<SimpleFsCommand> messaging) {
										StreamLZ4Decompressor lz4Decompressor = new StreamLZ4Decompressor(eventloop);
										messaging.binarySocketReader().streamTo(lz4Decompressor);
										callback.onResult(lz4Decompressor);
										messaging.shutdownWriter();
									}
								})
								.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> messaging) {
										callback.onException(new Exception(item.exceptionName));
										messaging.shutdown();
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception e) {
						callback.onException(e);
					}
				}
		);
	}

	@Override
	public void fileList(InetSocketAddress address, final ResultCallback<List<String>> callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<SimpleFsCommand>() {
									@Override
									public void onStart(Messaging<SimpleFsCommand> messaging) {
										SimpleFsCommand commandList = new SimpleFsCommandList();
										messaging.sendMessage(commandList);
									}
								})
								.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> output) {
										callback.onException(new Exception(item.exceptionName));
										output.shutdown();
									}
								})
								.addHandler(SimpleFsResponseFileList.class, new MessagingHandler<SimpleFsResponseFileList, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseFileList item, Messaging<SimpleFsCommand> output) {
										callback.onResult(item.fileList);
										output.shutdown();
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception e) {
						callback.onException(e);
					}
				}
		);
	}

	@Override
	public void deleteFile(InetSocketAddress address, final String fileName, final ResultCallback<Boolean> callback) {
		eventloop.connect(address, SocketSettings.defaultSocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<SimpleFsCommand>() {
									@Override
									public void onStart(Messaging<SimpleFsCommand> messaging) {
										SimpleFsCommand commandDelete = new SimpleFsCommandDelete(fileName);
										messaging.sendMessage(commandDelete);
										messaging.shutdown();
									}
								})
								.addHandler(SimpleFsResponseError.class, new MessagingHandler<SimpleFsResponseError, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseError item, Messaging<SimpleFsCommand> output) {
										output.shutdown();
										callback.onException(new Exception(item.exceptionName));
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception e) {
						callback.onException(e);
					}
				}
		);

	}

}
