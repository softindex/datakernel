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
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;

import static io.datakernel.async.Stages.repeat;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

public class AbstractServerTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testTimeouts() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		InetSocketAddress address = new InetSocketAddress("localhost", 5588);
		SocketSettings settings = SocketSettings.create().withImplReadTimeout(Duration.ofMillis(100000L)).withImplWriteTimeout(Duration.ofMillis(100000L));

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> repeat(() ->
						socket.read()
								.whenResult(buf -> {
									if (buf != null) {
										eventloop.delay(5, () ->
												socket.write(buf)
														.whenResult($ -> socket.close()));
									} else {
										socket.close();
									}
								})
								.toVoid()))
				.withSocketSettings(settings)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		AsyncTcpSocketImpl.connect(address)
				.whenResult(socket -> {
					socket.write(ByteBufStrings.wrapAscii("Hello!"))
							.thenCompose($ ->
									socket.read()
											.whenResult(System.out::println)
											.whenResult(buf -> {
												if (buf != null) {
													buf.recycle();
												}
											}))
							.whenComplete(($1, e1) -> socket.close());
				});

		eventloop.run();
	}
}
