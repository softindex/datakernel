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

import com.google.common.net.InetAddresses;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SimpleNioServer;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SimplePingTest {
	private static final int LISTEN_PORT = 1234;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	@Test
	public void testPing() throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		SimpleNioServer server = new SimpleNioServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamGsonDeserializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 16 * 1024),
						new StreamGsonSerializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 16 * 1024, 1024, 0))
						.addStarter(new MessagingStarter<SimpleFsResponse>() {
							@Override
							public void onStart(Messaging<SimpleFsResponse> messaging) {
								List<String> fileList = new ArrayList<>();
								fileList.add("test");
								messaging.sendMessage(new SimpleFsResponseFileList(fileList));
							}
						});
			}
		};

		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamGsonDeserializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 16 * 1024),
								new StreamGsonSerializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 16 * 1024, 1024, 0))
								.addHandler(SimpleFsResponseFileList.class, new MessagingHandler<SimpleFsResponseFileList, SimpleFsCommand>() {
									@Override
									public void onMessage(SimpleFsResponseFileList item, Messaging<SimpleFsCommand> output) {
										List<String> expected = Collections.singletonList("test");
										assertEquals(expected, item.fileList);
										output.shutdown();
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception exception) {
						fail("Test Exception: " + exception);
					}
				}
		);

		eventloop.run();
	}

}
