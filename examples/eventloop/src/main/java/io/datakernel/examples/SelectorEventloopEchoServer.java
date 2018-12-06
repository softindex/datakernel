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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioChannelEventHandler;
import io.datakernel.exception.ParseException;
import io.datakernel.net.ServerSocketSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.util.MemSize.kilobytes;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.nio.charset.StandardCharsets.UTF_8;


public class SelectorEventloopEchoServer {
	private static final InetSocketAddress ADDRESS = new InetSocketAddress(22233);
	private static final byte[] CRLF = {CR, LF};

	public static void main(String[] args) throws IOException {

		Eventloop eventloop = Eventloop.create().withCurrentThread();
		ByteBufsParser<ByteBuf> parser = ByteBufsParser.ofCrlfTerminatedBytes();
		eventloop.listen(ADDRESS, ServerSocketSettings.create(0), socketChannel -> {
			try {
				SocketAddress clientAddress = socketChannel.getRemoteAddress();
				System.out.println("Client connected: " + clientAddress);
				socketChannel.register(eventloop.getSelector(), OP_READ, new NioChannelEventHandler() {
					ByteBufQueue queue = new ByteBufQueue();
					ArrayDeque<ByteBuf> bufsToSend = new ArrayDeque<>();
					SelectionKey key;

					@Override
					public void onReadReady() {
						try {
							ByteBuf buf = ByteBufPool.allocate(kilobytes(16));
							ByteBuffer byteBuffer = buf.toWriteByteBuffer();
							int read = socketChannel.read(byteBuffer);
							if (read == -1) {
								socketChannel.close();
								queue.recycle();
								buf.recycle();
								return;
							}

							if (read == 0) {
								buf.recycle();
								return;
							}
							buf.ofWriteByteBuffer(byteBuffer);
							queue.add(buf);

							ByteBuf toSend;
							while ((toSend = parser.tryParse(queue)) != null) {
								System.out.printf("Received message from client(%s): %s\n", clientAddress, toSend.getString(UTF_8));
								bufsToSend.add(toSend);
							}

							if (!bufsToSend.isEmpty()) {
								if (key == null) {
									key = socketChannel.register(eventloop.getSelector(), OP_WRITE, this);
								} else {
									key.interestOps(OP_WRITE);
								}
							}
						} catch (IOException | ParseException e) {
							System.out.println("Failed to read data from network: " + e);
						}
					}

					@Override
					public void onWriteReady() {
						try {
							for (ByteBuf buf : bufsToSend) {
								buf = ByteBufPool.append(ByteBufStrings.wrapUtf8("Server: "), buf);
								buf = ByteBufPool.append(buf, CRLF);
								int written = socketChannel.write(buf.toReadByteBuffer());
								buf.moveReadPosition(written);
								if (buf.canRead()) {
									return;
								}
								buf.recycle();
								bufsToSend.remove(buf);
							}
							key.interestOps(OP_READ);
						} catch (IOException e) {
							System.out.println("Failed to write data to network:" + e);
						}
					}
				});
			} catch (IOException e) {
				System.out.println("Failed to register socket channel:" + e);
			}
		});

		System.out.println("Server is running");
		System.out.println("You can connect from telnet with command: telnet localhost 22233");

		eventloop.run();
	}
}
