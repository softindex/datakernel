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
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple echo server.
 */
public class TcpEchoServerExample {
	private static final int PORT = 9922;
	private static final byte[] CRLF = {CR, LF};

	/* Run server in an event loop. */
	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		SimpleServer server = SimpleServer.create(socket ->
				BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
						.parseStream(ByteBufsParser.ofCrlfTerminatedBytes())
						.peek(buf -> System.out.println("client:" + buf.getString(UTF_8)))
						.map(buf -> {
							ByteBuf serverBuf = ByteBufStrings.wrapUtf8("Server> ");
							return ByteBufPool.append(serverBuf, buf);
						})
						.map(buf -> ByteBufPool.append(buf, CRLF))
						.streamTo(ChannelConsumer.ofSocket(socket)))
				.withListenPort(PORT);

		server.listen();

		System.out.println("Server is running");
		System.out.println("You can connect from telnet with command: telnet localhost 9922 or by running TcpClientExample");

		eventloop.run();
	}
}


