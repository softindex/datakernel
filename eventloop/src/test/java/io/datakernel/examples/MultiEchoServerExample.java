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
import io.datakernel.eventloop.*;

import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;

public class MultiEchoServerExample extends AbstractServer {

	public class MultiEchoServerConnection extends TcpSocketConnection {
		protected MultiEchoServerConnection(Eventloop eventloop, SocketChannel socketChannel) {
			super(eventloop, socketChannel);
		}

		@Override
		public void onRegistered() {
			connections.add(this);
		}

		@Override
		public void onClosed() {
			connections.remove(this);
		}

		@Override
		protected void onRead() {
			while (!readQueue.isEmpty()) {
				ByteBuf buf = readQueue.take();
				for (MultiEchoServerConnection connection : connections) {
					ByteBuf sendBuf = buf.slice(buf.position(), buf.remaining());
					connection.write(sendBuf);
				}
			}
		}
	}

	private static List<MultiEchoServerConnection> connections = new LinkedList<>();

	public MultiEchoServerExample(Eventloop eventloop) {
		super(eventloop);
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		MultiEchoServerConnection connection = new MultiEchoServerConnection(eventloop, socketChannel);
		System.out.println("New client connection from: " + socketChannel);
		return connection;
	}

	public static void main(String[] args) throws Exception {
		final Eventloop eventloop = new Eventloop();

		MultiEchoServerExample echoServer = new MultiEchoServerExample(eventloop);
		echoServer.setListenPort(9922);

		System.out.println("Starting multi-echo/chat-room server...");
		echoServer.listen();

		System.out.println("Multi-echo/chat-room server started, try connecting to it by running multiple EchoConsoleClient.main() processes");
		eventloop.run();
	}
}
