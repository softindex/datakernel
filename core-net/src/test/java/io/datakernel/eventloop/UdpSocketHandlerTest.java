/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.eventloop;

import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncUdpSocket.EventHandler;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static org.junit.Assert.assertArrayEquals;

public final class UdpSocketHandlerTest {
	private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 45555);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	private final byte[] bytesToSend = {-127, 100, 0, 5, 11, 13, 17, 99};

	@Test
	public void testEchoUdpServer() throws IOException {
		SettablePromise<byte[]> receivePromise = new SettablePromise<>();

		DatagramChannel serverDatagramChannel = createDatagramChannel(DatagramSocketSettings.create(), SERVER_ADDRESS, null);
		AsyncUdpSocketImpl serverSocket = AsyncUdpSocketImpl.create(Eventloop.getCurrentEventloop(), serverDatagramChannel);
		serverSocket.setEventHandler(new EventHandler() {
			@Override
			public void onRegistered() {
				serverSocket.receive();
			}

			@Override
			public void onReceive(UdpPacket packet) {
				serverSocket.send(packet);
			}

			@Override
			public void onSend() {
				serverSocket.close();
			}

			@Override
			public void onClosedWithError(Exception e) {
				throw new AssertionError(e);
			}
		});
		serverSocket.register();

		DatagramChannel clientDatagramChannel = createDatagramChannel(DatagramSocketSettings.create(), null, null);
		AsyncUdpSocketImpl clientSocket = AsyncUdpSocketImpl.create(Eventloop.getCurrentEventloop(), clientDatagramChannel);
		clientSocket.setEventHandler(new EventHandler() {
			@Override
			public void onRegistered() {
				clientSocket.send(UdpPacket.of(ByteBuf.wrapForReading(bytesToSend), SERVER_ADDRESS));
			}

			@Override
			public void onReceive(UdpPacket packet) {
				byte[] bytesReceived = packet.getBuf().array();
				byte[] message = new byte[packet.getBuf().readRemaining()];

				System.arraycopy(bytesReceived, 0, message, 0, packet.getBuf().readRemaining());
				receivePromise.set(message);

				packet.recycle();
				clientSocket.close();
			}

			@Override
			public void onSend() {
				clientSocket.receive();
			}

			@Override
			public void onClosedWithError(Exception e) {
				throw new AssertionError(e);
			}
		});
		clientSocket.register();

		assertArrayEquals(bytesToSend, await(receivePromise));
	}

}
