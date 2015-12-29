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
import io.datakernel.net.SocketSettings;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static org.junit.Assert.*;

public class PingPongSocketConnectionTest {
	public static final int PORT = 9921;
	public static final String PING = "PING";
	public static final String PONG = "PONG";
	public static final int ITERATIONS = 3;

	static class PingPongConnection extends TcpSocketConnection {
		private int countdown;
		private final byte[] request;
		private final byte[] response;
		private final boolean client;

		private PingPongConnection(NioEventloop eventloop, SocketChannel socketChannel, String request, String response, int countdown, boolean client) {
			super(eventloop, socketChannel);
			this.countdown = countdown;
			this.request = encodeAscii(request);
			this.response = encodeAscii(response);
			this.client = client;
		}

		public static PingPongConnection clientConnection(NioEventloop eventloop, SocketChannel socketChannel, String ping, String pong, int countdown) {
			return new PingPongConnection(eventloop, socketChannel, pong, ping, countdown, true);
		}

		public static PingPongConnection serverConnection(NioEventloop eventloop, SocketChannel socketChannel, String ping, String pong) {
			return new PingPongConnection(eventloop, socketChannel, ping, pong, 0, false);
		}

		@Override
		public void onRegistered() {
			if (client) {
				write(ByteBuf.wrap(response));
			}
		}

		@Override
		protected void onRead() {
			int readBytes = readQueue.remainingBytes();
			if (readBytes < request.length) {
				return;
			}
			assertEquals(request.length, readBytes);

			ByteBuf requestBuf = ByteBuf.allocate(readBytes);
			readQueue.drainTo(requestBuf);
			requestBuf.flip();
			System.out.println("Received from " + (client ? "server" : "client") + ": " + decodeAscii(requestBuf));
			assertTrue(requestBuf.equalsTo(request));

			if (client && --countdown == 0) {
				close();
				return;
			}

			write(ByteBuf.wrap(response));
		}
	}

	static class TestServer extends SimpleNioServer {
		public TestServer(NioEventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected SocketConnection createConnection(SocketChannel socketChannel) {
			return PingPongConnection.serverConnection(eventloop, socketChannel, PING, PONG);
		}
	}

	@Test
	public void testPingPong() throws IOException {
		final NioEventloop eventloop = new NioEventloop();
		TestServer server = new TestServer(eventloop);
		server.acceptOnce().setListenPort(PORT);

		server.listen();

		eventloop.connect(new InetSocketAddress("localhost", PORT), new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						PingPongConnection pingPongConnection = PingPongConnection.clientConnection(eventloop, socketChannel, PING, PONG, ITERATIONS);
						pingPongConnection.register();
					}

					@Override
					public void onException(Exception exception) {
						fail("Exception: " + exception);
					}
				}
		);

		eventloop.run();
	}
}
