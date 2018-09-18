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

import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.async.Stages.loop;
import static io.datakernel.async.Stages.repeat;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

public class PingPongSocketConnectionTest {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);
	private final int ITERATIONS = 3;
	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(4)
			.andThen(buf -> {
				byte[] bytes = buf.asArray();
				buf.recycle();
				return bytes;
			})
			.andThen(ByteBufStrings::decodeAscii);

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> {
					ByteBufsSupplier bufsSupplier = ByteBufsSupplier.of(socket.reader());
					repeat(() ->
							PARSER.parse(bufsSupplier)
									.whenResult(System.out::println)
									.thenCompose($ -> socket.write(wrapAscii(RESPONSE_MSG))))
							.thenRunEx(socket::close);
				})
				.withListenAddress(ADDRESS)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(ADDRESS)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					ByteBufsSupplier bufsSupplier = ByteBufsSupplier.of(socket.reader());
					loop(3, i -> i != 0,
							i -> socket.write(wrapAscii(REQUEST_MSG))
									.thenCompose($ -> PARSER.parse(bufsSupplier)
											.whenResult(System.out::println)
											.thenApply($2 -> i - 1)))
							.thenRunEx(socket::close);
				})
				.whenException(e -> { throw new RuntimeException(e); });

		eventloop.run();
	}

}
