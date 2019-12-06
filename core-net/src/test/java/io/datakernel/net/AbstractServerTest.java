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

package io.datakernel.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.common.ref.RefLong;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class AbstractServerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testTimeouts() throws IOException {
		String message = "Hello!";
		InetSocketAddress address = new InetSocketAddress("localhost", getFreePort());
		SocketSettings settings = SocketSettings.create().withImplReadTimeout(Duration.ofMillis(100000L)).withImplWriteTimeout(Duration.ofMillis(100000L));

		RefLong delay = new RefLong(5);
		SimpleServer.create(socket -> {
			Promises.<ByteBuf>until(null, $ ->
							socket.read()
									.whenResult(buf -> {
										getCurrentEventloop().delay(delay.inc(), () -> {
											socket.write(buf)
													.whenComplete(($2, e) -> {
														if (buf == null) {
															socket.close();
														}
													});
										});
									}),
					Objects::isNull);
		})
				.withSocketSettings(settings)
				.withListenAddress(address)
				.withAcceptOnce()
				.listen();

		ByteBuf response = await(AsyncTcpSocketImpl.connect(address)
				.then(socket ->
						socket.write(ByteBufStrings.wrapAscii(message))
								.then($ -> socket.write(null))
								.then($ -> {
									ByteBufQueue queue = new ByteBufQueue();
									return Promises.<ByteBuf>until(null,
											$2 -> socket.read()
													.then(buf -> {
														if (buf != null) {
															queue.add(buf);
														}
														return Promise.of(buf);
													}),
											Objects::isNull)
											.map($2 -> queue.takeRemaining());
								})
								.whenComplete(socket::close)));

		assertEquals(message, response.asString(UTF_8));
	}
}
