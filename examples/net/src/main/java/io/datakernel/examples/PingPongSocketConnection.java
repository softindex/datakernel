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

import io.datakernel.async.AsyncPredicate;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.async.Promises.loop;
import static io.datakernel.async.Promises.repeat;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PingPongSocketConnection {
	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);
	private static final int ITERATIONS = 3;
	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(4)
			.andThen(buf -> buf.asString(UTF_8));

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		SimpleServer server = SimpleServer.create(
				socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
					repeat(() ->
							bufsSupplier.parse(PARSER)
									.accept(System.out::println)
									.then($ -> socket.write(wrapAscii(RESPONSE_MSG))))
							.acceptEx(($, e) -> socket.close());
				})
				.withListenAddress(ADDRESS)
				.withAcceptOnce();

		server.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.accept(socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
					loop(0, AsyncPredicate.of(i -> i < ITERATIONS),
							i -> socket.write(wrapAscii(REQUEST_MSG))
									.then($ -> bufsSupplier.parse(PARSER)
											.accept(System.out::println)
											.map($2 -> i + 1)))
							.acceptEx(($, e) -> socket.close());
				})
				.acceptEx(Exception.class, e -> { throw new RuntimeException(e); });

		eventloop.run();
	}
}
