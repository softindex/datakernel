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

package io.datakernel.hashfs.stub.upload_big;

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ForwardingConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.hashfs.protocol.gson.commands.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

class ClientProtocol {

	private static final Logger logger = LoggerFactory.getLogger(ClientProtocol.class);

	private final NioEventloop eventloop;

	public ClientProtocol(NioEventloop eventloop) {
		this.eventloop = eventloop;
	}

	private StreamMessagingConnection<HashFsResponse, HashFsCommand> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsResponseSerialization.GSON, HashFsResponse.class, 16 * 1024),
				new StreamGsonSerializer<>(eventloop, HashFsCommandSerialization.GSON, HashFsCommand.class, 16 * 1024, 1024, 0));
	}

	public void upload(final InetSocketAddress address, final String destinationName, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		connect(address, new ForwardingConnectCallback(callback) {

					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = createConnection(socketChannel)
								.addStarter(new MessagingStarter<HashFsCommand>() {
									@Override
									public void onStart(Messaging<HashFsCommand> messaging) {
										HashFsCommand commandUpload = new HashFsCommandUpload(destinationName);
										messaging.sendMessage(commandUpload);
										callback.onResult(messaging.binarySocketWriter());
									}
								})
								.addHandler(HashFsResponseError.class, new MessagingHandler<HashFsResponseError, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseError item, Messaging<HashFsCommand> messaging) {
										logger.warn("Receive error from server {}", item.exceptionName);
										messaging.shutdown();
										callback.onException(new Exception(item.exceptionName));
									}
								})
								.addHandler(HashFsResponseFileUploaded.class, new MessagingHandler<HashFsResponseFileUploaded, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseFileUploaded item, Messaging<HashFsCommand> messaging) {
										logger.info("Client receive file {} uploaded", destinationName);
										messaging.shutdown();
									}
								});
						connection.register();
					}
				}
		);
	}

	private void connect(InetSocketAddress address, final ForwardingConnectCallback connectCallback) {
		eventloop.connect(address, new SocketSettings(), connectCallback);
	}

	public void getAliveServers(InetSocketAddress address, final ResultCallback<List<ServerInfo>> callback) {
		connect(address, new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<HashFsCommand>() {
							@Override
							public void onStart(Messaging<HashFsCommand> messaging) {
								messaging.sendMessage(new HashFsCommandAliveServers());
							}
						})
						.addHandler(HashFsResponseAliveServers.class, new MessagingHandler<HashFsResponseAliveServers, HashFsCommand>() {
							@Override
							public void onMessage(HashFsResponseAliveServers item, Messaging<HashFsCommand> messaging) {
								messaging.shutdown();
								List<ServerInfo> aliveServers = new ArrayList<>();
								callback.onResult(aliveServers);
								messaging.shutdown();
							}
						});
				connection.register();
			}
		});
	}

}
