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

package io.datakernel.hashfs.stub;

import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.protocol.gson.commands.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * This class currently need just for test.
 */
public class ServerChangesAskGson {

	private final NioEventloop eventloop;

	private ServerChangesAskGson(NioEventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static ServerChangesAskGson createServerChangesAsk(NioEventloop eventloop, InetSocketAddress address, final ServerStatusListener serverStatusListener) {
		final ServerChangesAskGson changesAsk = new ServerChangesAskGson(eventloop);

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = changesAsk.createConnection(socketChannel)
								.addStarter(new MessagingStarter<HashFsCommand>() {
									@Override
									public void onStart(Messaging<HashFsCommand> messaging) {
//										messaging.sendMessage(new HashFsCommandSubscribe());
									}
								})
								.addHandler(HashFsResponseForListeners.class, new MessagingHandler<HashFsResponseForListeners, HashFsCommand>() {
									@Override
									public void onMessage(HashFsResponseForListeners item, Messaging<HashFsCommand> messaging) {
										switch (item.operation) {
											case UPLOADED:
												serverStatusListener.onFileUploaded(item.serverId, item.fileName);
												break;
											case DELETED_BY_SERVER:
												serverStatusListener.onFileDeletedByServer(item.serverId, item.fileName);
												break;
											case DELETED_BY_USER:
												serverStatusListener.onFileDeletedByUser(item.serverId, item.fileName);
												break;
										}
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception exception) {

					}
				}
		);

		return changesAsk;
	}

	private StreamMessagingConnection<HashFsResponse, HashFsCommand> createConnection(SocketChannel socketChannel) {

		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsResponseSerialization.GSON, HashFsResponse.class, 16 * 1024),
				new StreamGsonSerializer<>(eventloop, HashFsCommandSerialization.GSON, HashFsCommand.class, 16 * 1024, 1024, 0));
	}

}
