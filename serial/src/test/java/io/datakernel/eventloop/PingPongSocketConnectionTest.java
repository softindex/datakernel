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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.async.Promises.loop;
import static io.datakernel.async.Promises.repeat;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

public class PingPongSocketConnectionTest {
	private final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);
	private final int ITERATIONS = 3;
	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(4)
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> {
					ByteBufsSupplier bufsSupplier = ByteBufsSupplier.of(SerialSupplier.ofSocket(socket));
					repeat(() ->
							PARSER.parse(bufsSupplier)
									.whenResult(System.out::println)
									.thenCompose($ -> socket.write(wrapAscii(RESPONSE_MSG))))
							.whenComplete(($, e) -> socket.close());
				})
				.withListenAddress(ADDRESS)
				.withAcceptOnce();

		server.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.whenResult(socket -> {
					ByteBufsSupplier bufsSupplier = ByteBufsSupplier.of(SerialSupplier.ofSocket(socket));
					loop(ITERATIONS, i -> i != 0,
							i -> socket.write(wrapAscii(REQUEST_MSG))
									.thenCompose($ -> PARSER.parse(bufsSupplier)
											.whenResult(System.out::println)
											.thenApply($2 -> i - 1)))
							.whenComplete(($, e) -> socket.close());
				})
				.whenException(e -> { throw new RuntimeException(e); });

		eventloop.run();
	}

}
