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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple TCP console client.
 * By default this client connects to the same address as the server in the previous example.
 */
public class TcpClientExample {
	private final Eventloop eventloop = Eventloop.create();
	private AsyncTcpSocket socket;
	static {
		LoggerConfigurer.enableLogging();
	}

	/* Thread, which sends characters and prints received responses to the console. */
	private Thread getScannerThread() {
		return new Thread(() -> {
			Scanner scanIn = new Scanner(System.in);
			while (true) {
				String line = scanIn.nextLine();
				if (line.isEmpty()) {
					break;
				}
				ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(line + "\r\n"));
				eventloop.execute(() -> socket.write(buf));
			}
			eventloop.execute(socket::close);
		});
	}

	private void run() {
		System.out.println("Connecting to server at localhost (port 9922)...");
		eventloop.connect(new InetSocketAddress("localhost", 9922), new ConnectCallback() {
			@Override
			public void onConnect(@NotNull SocketChannel socketChannel) {
				System.out.println("Connected to server, enter some text and send it by pressing 'Enter'.");
				socket = AsyncTcpSocket.ofSocketChannel(socketChannel);

				BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
						.parseStream(ByteBufsParser.ofCrlfTerminatedBytes())
						.streamTo(ChannelConsumer.ofConsumer(buf -> System.out.println(buf.asString(UTF_8))));

				getScannerThread().start();
			}

			@Override
			public void onException(@NotNull Throwable e) {
				System.out.printf("Could not connect to server, make sure it is started: %s\n", e);
			}
		});

		eventloop.run();
	}

	public static void main(String[] args) {
		new TcpClientExample().run();
	}
}
