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

package io.datakernel.examples;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.ServerSocketSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Poor implementation of echo server that is looping infinitely while trying to receive data from socket
 */
public class BusyWaitEventloopEchoServer {
	public static final InetSocketAddress ADDRESS = new InetSocketAddress(22233);

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		eventloop.listen(ADDRESS, ServerSocketSettings.create(0), socketChannel -> {
			try {
				SocketAddress clientAddress = socketChannel.getRemoteAddress();
				System.out.println("Client connected: " + clientAddress);
				ByteBuffer bufferIn = ByteBuffer.allocate(1024);
				while (true) {
					int read = socketChannel.read(bufferIn);

					if (read == 0) {
						continue;
					} else if (read == -1) {
						System.out.println("Client disconnected: " + clientAddress);
						socketChannel.close();
						return;
					}

					String message = StandardCharsets.UTF_8.decode((ByteBuffer) bufferIn.flip()).toString();
					System.out.printf("Received message from client(%s): %s", clientAddress, message);

					String stringIn = "Server: " + message;


					socketChannel.write(ByteBuffer.wrap(stringIn.getBytes()));
					bufferIn.clear();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

		System.out.println("Server is running");
		System.out.println("You can connect from telnet with command: telnet localhost 22233");

		eventloop.run();
	}
}
