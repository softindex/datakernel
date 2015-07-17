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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SimpleNioServer;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.eventloop.TcpSocketConnection;

import java.nio.channels.SocketChannel;

import static io.datakernel.util.ByteBufStrings.decodeAscii;

/**
 * Example of creating a simple echo server by subclassing `SimpleNioServer`.
 */
public class TcpEchoServerExample {
	public static final int PORT = 9922;

	/* TCP server that prints received data to console and sends it back to sender. */
	public static class EchoServer extends SimpleNioServer {
		public EchoServer(NioEventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected SocketConnection createConnection(SocketChannel socketChannel) {
			return new EchoServerConnection(eventloop, socketChannel);
		}

		private class EchoServerConnection extends TcpSocketConnection {
			protected EchoServerConnection(NioEventloop eventloop, SocketChannel socketChannel) {
				super(eventloop, socketChannel);
			}

			@Override
			protected void onReadEndOfStream() {
				System.out.println("client: EOS");
				close();
			}

			@Override
			protected void onRead() {
				while (!readQueue.isEmpty()) {
					ByteBuf buf = readQueue.take();
					System.out.println("client: " + decodeAscii(buf));
					write(buf);
				}
			}
		}
	}

	/* Run server in an event loop. */
	public static void main(String[] args) throws Exception {
		NioEventloop eventloop = new NioEventloop();

		EchoServer echoServer = new EchoServer(eventloop);
		echoServer.setListenPort(PORT).acceptOnce();

		echoServer.listen();

		System.out.println("TCP echo server is listening at port " + PORT);
		eventloop.run();
	}
}


