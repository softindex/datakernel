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

import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.async.Promises.loop;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class PingPongSocketConnectionTest {
	private static final int ITERATIONS = 100;

	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);

	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(4)
			.andThen(buf -> buf.asString(UTF_8));

	@Test
	public void test() throws IOException {
		SimpleServer.create(socket -> {
			ByteBufsSupplier bufsSupplier = ByteBufsSupplier.of(SerialSupplier.ofSocket(socket));
			loop(ITERATIONS, i -> i != 0, i ->
					PARSER.parse(bufsSupplier)
							.whenResult(res -> assertEquals(REQUEST_MSG, res))
							.thenCompose($ -> socket.write(wrapAscii(RESPONSE_MSG)))
							.thenApply($ -> i - 1))
					.whenComplete(($, e) -> socket.close())
					.whenComplete(assertComplete());
		})
				.withListenAddress(ADDRESS)
				.withAcceptOnce()
				.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenCompose(socket -> {
					ByteBufsSupplier bufsSupplier = ByteBufsSupplier.of(SerialSupplier.ofSocket(socket));
					return loop(ITERATIONS, i -> i != 0,
							i -> socket.write(wrapAscii(REQUEST_MSG))
									.thenCompose($ -> PARSER.parse(bufsSupplier))
									.whenResult(res -> assertEquals(RESPONSE_MSG, res))
									.thenApply($ -> i - 1))
							.whenComplete(($, e) -> socket.close())
							.whenComplete(assertComplete());
				})
				.whenComplete(assertComplete());
	}
}
