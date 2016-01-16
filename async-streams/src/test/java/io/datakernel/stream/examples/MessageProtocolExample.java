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

package io.datakernel.stream.examples;

import com.google.common.net.InetAddresses;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleEventloopServer;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static org.junit.Assert.fail;

/**
 * Example 7.
 * Example of working with TcpStreamSocketConnection and MessageProtocol.
 */
public class MessageProtocolExample {
	private static final int LISTEN_PORT = 1234;
	private static final InetSocketAddress address =
			new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	/* Subclass of SimpleNioServer with BinaryProtocolMessaging, which sends received bytes back. */
	public static class MessageProtocolServer extends SimpleEventloopServer {
		public MessageProtocolServer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected SocketConnection createConnection(SocketChannel socketChannel) {
			return new StreamMessagingConnection<>(eventloop, socketChannel,
					new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10),
					new StreamBinarySerializer<>(eventloop, intSerializer(), 2, 10, 0, false))
					.addHandler(Integer.class, new MessagingHandler<Integer, Integer>() {
						@Override
						public void onMessage(Integer item, Messaging<Integer> messaging) {
							messaging.sendMessage(item);
						}
					});
		}
	}

	/* Create the client connection with MessageProtocol and MessageProtocol.Starter.
	Starter sends the first integer to server after the start up.
	Each received integer is decremented and sent back, until the negative integer is received. */
	public static void createClientConnection(final Eventloop eventloop) {
		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10),
								new StreamBinarySerializer<>(eventloop, intSerializer(), 2, 10, 0, false))
								.addStarter(new MessagingStarter<Integer>() {
									@Override
									public void onStart(Messaging<Integer> messaging) {
										messaging.sendMessage(10);
									}
								})
								.addHandler(Integer.class, new MessagingHandler<Integer, Integer>() {
									@Override
									public void onMessage(Integer item, Messaging<Integer> messaging) {
										if (item > 0) {
											messaging.sendMessage(item - 1);
										} else {
											messaging.shutdown();
										}
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception exception) {
						fail("Exception: " + exception);
					}
				}
		);
	}

	/* Run the server and client in an event loop. */
	public static void main(String[] args) throws IOException {
		final Eventloop eventloop = new Eventloop();

		SimpleEventloopServer server = new MessageProtocolServer(eventloop);
		server.setListenAddress(address).acceptOnce();
		server.listen();
		createClientConnection(eventloop);

		eventloop.run();
	}
}