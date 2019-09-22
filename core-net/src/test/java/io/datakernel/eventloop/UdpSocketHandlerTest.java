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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.net.DatagramSocketSettings;
import io.datakernel.net.AsyncUdpSocketImpl;
import io.datakernel.net.UdpPacket;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;

import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.assertComplete;
import static org.junit.Assert.assertArrayEquals;

public final class UdpSocketHandlerTest {
	private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 45555);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final byte[] bytesToSend = {-127, 100, 0, 5, 11, 13, 17, 99};

	@Test
	public void testEchoUdpServer() throws IOException {
		DatagramChannel serverDatagramChannel = createDatagramChannel(DatagramSocketSettings.create(), SERVER_ADDRESS, null);

		AsyncUdpSocketImpl.connect(Eventloop.getCurrentEventloop(), serverDatagramChannel)
				.then(serverSocket -> serverSocket.receive()
						.then(serverSocket::send)
						.whenComplete(serverSocket::close))
				.whenComplete(assertComplete());

		DatagramChannel clientDatagramChannel = createDatagramChannel(DatagramSocketSettings.create(), null, null);

		Promise<AsyncUdpSocketImpl> promise = AsyncUdpSocketImpl.connect(Eventloop.getCurrentEventloop(), clientDatagramChannel)
				.whenComplete(assertComplete(clientSocket -> {

					clientSocket.send(UdpPacket.of(ByteBuf.wrapForReading(bytesToSend), SERVER_ADDRESS))
							.whenComplete(assertComplete());

					clientSocket.receive()
							.whenComplete(clientSocket::close)
							.whenComplete(assertComplete(packet -> {
								byte[] message = packet.getBuf().asArray();

								assertArrayEquals(bytesToSend, message);

								System.out.println("message = " + Arrays.toString(message));
							}));
				}));

		await(promise);
	}

}
