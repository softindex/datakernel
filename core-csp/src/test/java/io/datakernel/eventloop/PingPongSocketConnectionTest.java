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

import io.datakernel.async.AsyncPredicate;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.async.Promises.loop;
import static io.datakernel.async.TestUtils.await;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class PingPongSocketConnectionTest {
	private static final int ITERATIONS = 100;

	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);

	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(4)
			.andThen(buf -> buf.asString(UTF_8));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Test
	public void test() throws IOException {
		SimpleServer.create(
				socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
					loop(ITERATIONS, AsyncPredicate.of(i -> i != 0),
							i -> bufsSupplier.parse(PARSER)
									.whenResult(res -> assertEquals(REQUEST_MSG, res))
									.then($ -> socket.write(wrapAscii(RESPONSE_MSG)))
									.map($ -> i - 1))
							.whenComplete(($, e) -> socket.close())
							.whenComplete(assertComplete());
				})
				.withListenAddress(ADDRESS)
				.withAcceptOnce()
				.listen();

		await(AsyncTcpSocketImpl.connect(ADDRESS)
				.then(socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
					return loop(ITERATIONS, AsyncPredicate.of(i -> i != 0),
							i -> socket.write(wrapAscii(REQUEST_MSG))
									.then($ -> bufsSupplier.parse(PARSER))
									.whenResult(res -> assertEquals(RESPONSE_MSG, res))
									.map($ -> i - 1))
							.whenResult($ -> socket.close());
				}));
	}
}
