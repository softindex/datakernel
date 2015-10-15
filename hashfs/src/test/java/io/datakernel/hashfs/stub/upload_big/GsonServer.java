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

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.protocol.gson.commands.*;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class GsonServer extends AbstractNioServer<GsonServer> {

	private final NioEventloop eventloop;
	private final Server fileServer;

	private GsonServer(NioEventloop eventloop, Server fileServer) {
		super(eventloop);
		this.fileServer = fileServer;
		this.eventloop = eventloop;
	}

	public static GsonServer createServerTransport(InetSocketAddress address, NioEventloop eventloop, Server fileServer) throws IOException {
		GsonServer serverTransport = new GsonServer(eventloop, fileServer);
		serverTransport.setListenPort(address.getPort());
		serverTransport.acceptOnce(false);
		serverTransport.listen();
		return serverTransport;
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {

		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsCommandSerialization.GSON, HashFsCommand.class, 16 * 1024),
				new StreamGsonSerializer<>(eventloop, HashFsResponseSerialization.GSON, HashFsResponse.class, 16 * 1024, 1024, 1))
				.addHandler(HashFsCommandUpload.class, new MessagingHandler<HashFsCommandUpload, HashFsResponse>() {
					@Override
					public void onMessage(final HashFsCommandUpload item, final Messaging<HashFsResponse> messaging) {

						StreamProducer<ByteBuf> producer = messaging.read();
						fileServer.onUpload(item.filename, producer, new CompletionCallback() {
							@Override
							public void onComplete() {
								messaging.sendMessage(new HashFsResponseFileUploaded());
								messaging.shutdown();
							}

							@Override
							public void onException(Exception exception) {
								messaging.sendMessage(new HashFsResponseError(exception.getMessage()));
								messaging.shutdown();
							}
						});
					}
				})
				.addHandler(HashFsCommandAliveServers.class, new MessagingHandler<HashFsCommandAliveServers, HashFsResponse>() {
					@Override
					public void onMessage(HashFsCommandAliveServers item, final Messaging<HashFsResponse> messaging) {
						List<Integer> aliveServersId = new ArrayList<>();
						messaging.sendMessage(new HashFsResponseAliveServers(aliveServersId));
					}
				});
	}

}
