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

package io.datakernel.examples;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.UdpPacket;
import io.datakernel.eventloop.UdpSocketConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;

/**
 * Example of creating UDP echo server and UDP client by subclassing `UDPSocketConnection`.
 */
public class UdpEchoServerClientExample {
	private static final int SERVER_PORT = 45555;
	private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("127.0.0.1", SERVER_PORT);
	private static final byte[] bytesToSend = new byte[]{-127, 100, 0, 5, 11, 13, 17, 99};

	/* UDP server, which sends received packets back to sender. */
	private static class EchoServerUdpConnection extends UdpSocketConnection {
		public EchoServerUdpConnection(Eventloop eventloop, DatagramChannel datagramChannel) {
			super(eventloop, datagramChannel);
		}

		@Override
		protected void onRead(UdpPacket packet) {
			System.out.println("Server read completed from port " + packet.getSocketAddress().getPort());
			send(packet);
		}

		@Override
		protected void onWriteFlushed() {
			System.out.println("Server write completed");
			close();
		}
	}

	/* UDP client, which sends test UDP packet to server and outputs received bytes to console. */
	private static class ClientUdpConnection extends UdpSocketConnection {
		public ClientUdpConnection(Eventloop eventloop, DatagramChannel datagramChannel) {
			super(eventloop, datagramChannel);
		}

		@Override
		protected void onRead(UdpPacket packet) {
			System.out.println("Client read completed");

			byte[] bytesReceived = packet.getBuf().array();

			System.out.print("Received " + packet.getBuf().limit() + " bytes: ");
			for (int i = 0; i < packet.getBuf().limit(); ++i) {
				if (i > 0) {
					System.out.print(", ");
				}
				System.out.print(bytesReceived[i]);
			}

			packet.recycle();

			close();
		}

		@Override
		protected void onWriteFlushed() {
			System.out.println("Client write completed");
		}
	}

	/* Run server and client in an event loop. */
	public static void main(String[] args) throws IOException {
		Eventloop eventloop = new Eventloop();

		DatagramChannel serverChannel = createDatagramChannel(defaultDatagramSocketSettings(),
				SERVER_ADDRESS, null);
		EchoServerUdpConnection serverConnection = new EchoServerUdpConnection(eventloop, serverChannel);
		serverConnection.register();

		// client
		DatagramChannel clientChannel = createDatagramChannel(defaultDatagramSocketSettings(), null, null);
		ClientUdpConnection clientConnection = new ClientUdpConnection(eventloop, clientChannel);
		clientConnection.register();

		clientConnection.send(new UdpPacket(ByteBuf.wrap(bytesToSend), SERVER_ADDRESS));

		eventloop.run();
	}
}