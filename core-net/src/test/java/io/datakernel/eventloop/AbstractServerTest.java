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
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.net.AsyncTcpSocketImpl;
import io.datakernel.net.SimpleServer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.promise.Promises.repeat;
import static io.datakernel.promise.TestUtils.await;
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
		InetSocketAddress address = new InetSocketAddress("localhost", 5588);
		SocketSettings settings = SocketSettings.create().withImplReadTimeout(Duration.ofMillis(100000L)).withImplWriteTimeout(Duration.ofMillis(100000L));

		SimpleServer.create(socket ->
				repeat(() ->
						socket.read()
								.whenResult(buf -> {
									if (buf == null) {
										socket.close();
										return;
									}
									getCurrentEventloop().delay(5L, () ->
											socket.write(buf)
													.whenResult($ -> socket.close()));
								})
								.toVoid()))
				.withSocketSettings(settings)
				.withListenAddress(address)
				.withAcceptOnce()
				.listen();

		ByteBuf response = await(AsyncTcpSocketImpl.connect(address)
				.then(socket ->
						socket.write(ByteBufStrings.wrapAscii(message))
								.then($ -> socket.read())
								.whenComplete(socket::close)));

		assertEquals(message, response.asString(UTF_8));
	}
}
