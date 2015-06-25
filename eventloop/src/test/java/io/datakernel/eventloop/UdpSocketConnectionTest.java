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

package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

import static io.datakernel.eventloop.NioEventloop.createDatagramChannel;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static org.junit.Assert.assertArrayEquals;

public class UdpSocketConnectionTest {
	private static final int SERVER_PORT = 45555;
	private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("127.0.0.1", SERVER_PORT);
	private NioEventloop eventloop = new NioEventloop();

	private final byte[] bytesToSend = new byte[]{-127, 100, 0, 5, 11, 13, 17, 99};

	private class ClientUdpConnection extends UdpSocketConnection {
		public ClientUdpConnection(NioEventloop eventloop, DatagramChannel datagramChannel) {
			super(eventloop, datagramChannel);
		}

		@Override
		protected void onRead(UdpPacket packet) {
			System.out.println("Client read completed");

			byte[] bytesReceived = packet.getBuf().array();
			byte[] message = new byte[packet.getBuf().position()];

			System.out.print("Received " + packet.getBuf().position() + " bytes: ");
			for (int i = 0; i < packet.getBuf().position(); ++i) {
				message[i] = bytesReceived[i];

				System.out.print(bytesReceived[i]);
				if (i != packet.getBuf().position() - 1) {
					System.out.print(", ");
				}
			}

			System.out.println("");

			assertArrayEquals(bytesToSend, message);

			close();
		}

		public void sendTestData(byte[] data, InetSocketAddress address) {
			send(new UdpPacket(ByteBuf.wrap(data), address));

			System.out.print("Sent " + data.length + " bytes: ");
			for (int i = 0; i < data.length; ++i) {
				System.out.print(data[i]);
				if (i != data.length - 1) {
					System.out.print(", ");
				}
			}
			System.out.println("");
		}

		@Override
		protected void onWriteFlushed() {
			System.out.println("Client write completed");
		}
	}

	private class EchoServerUdpConnection extends UdpSocketConnection {
		public EchoServerUdpConnection(NioEventloop eventloop, DatagramChannel datagramChannel) {
			super(eventloop, datagramChannel);
		}

		@Override
		protected void onRead(UdpPacket packet) {
			System.out.println("Server read completed from port " + packet.getSocketAddress().getPort());
			packet.getBuf().flip();
			send(packet);
		}

		@Override
		protected void onWriteFlushed() {
			System.out.println("Server write completed");
			close();
		}
	}

	@Test
	public void testEchoUdpServer() throws Exception {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				try {
					//  server
					DatagramChannel serverChannel = createDatagramChannel(defaultDatagramSocketSettings(), SERVER_ADDRESS, null);
					EchoServerUdpConnection serverConnection = new EchoServerUdpConnection(eventloop, serverChannel);
					serverConnection.register();

					// client
					DatagramChannel clientChannel = createDatagramChannel(defaultDatagramSocketSettings(), null, null);
					ClientUdpConnection clientConnection = new ClientUdpConnection(eventloop, clientChannel);
					clientConnection.register();

					clientConnection.sendTestData(bytesToSend, SERVER_ADDRESS);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		eventloop.run();
	}
}