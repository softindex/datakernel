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
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;

import static io.datakernel.async.Promises.repeat;
import static io.datakernel.async.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class AbstractServerTest {

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
									Eventloop.getCurrentEventloop().delay(5, () ->
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
								.whenComplete(($, e) -> socket.close())));

		assertEquals(message, response.asString(UTF_8));
	}
}
