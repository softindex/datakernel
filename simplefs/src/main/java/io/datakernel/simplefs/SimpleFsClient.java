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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SimpleFsClient implements SimpleFs {

	private final NioEventloop eventloop;

	public SimpleFsClient(NioEventloop eventloop) {
		this.eventloop = eventloop;
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
										callback.onResult(messaging.binarySocketWriter());
										messaging.shutdownReader();
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
										callback.onResult(messaging.binarySocketReader());
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
