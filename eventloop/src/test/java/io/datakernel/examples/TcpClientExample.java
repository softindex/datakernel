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
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.eventloop.TcpSocketConnection;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.util.ByteBufStrings.decodeUTF8;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * Example of creating a simple TCP console client by subclassing `TcpSocketConnection`.
 * By default this client connects to the same address as the server in the previous example.
 */
public class TcpClientExample {
	/* TCP client connection, which sends characters and prints received responses to the console. */
	private static class EchoConsoleClientConnection extends TcpSocketConnection {
		public EchoConsoleClientConnection(NioEventloop eventloop, SocketChannel socketChannel) {
			super(eventloop, socketChannel);
		}

		@Override
		public void onRegistered() {
			System.out.println("Client connected, enter some text and send it by pressing 'Enter'.");
			new Thread(new Runnable() {
				@Override
				public void run() {
					Scanner scanIn = new Scanner(System.in);
					while (true) {
						String line = scanIn.nextLine();
						if (line.isEmpty())
							break;
						final ByteBuf buf = ByteBuf.wrap(encodeAscii(line));
						eventloop.postConcurrently(new Runnable() {
							@Override
							public void run() {
								write(buf);
							}
						});
					}
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							close();
						}
					});
				}
			}).start();
		}

		@Override
		protected void onRead() {
			while (!readQueue.isEmpty()) {
				ByteBuf buffer = readQueue.take();
				System.out.println("> " + decodeUTF8(buffer));
			}
		}
	}

	/* Run TCP client in an event loop. */
	public static void main(String[] args) throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		System.out.println("Connecting to server at localhost (port 9922)...");
		eventloop.connect(new InetSocketAddress("localhost", 9922), defaultSocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection socketConnection = new EchoConsoleClientConnection(eventloop,
								socketChannel);
						socketConnection.register();
					}

					@Override
					public void onException(Exception exception) {
						System.err.println("Could not connect to server, make sure it is started: \n" + exception);
					}
				}
		);

		eventloop.run();
	}
}
