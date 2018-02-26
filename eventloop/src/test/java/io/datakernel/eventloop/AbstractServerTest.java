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
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.fail;

public class AbstractServerTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testTimeouts() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		InetSocketAddress address = new InetSocketAddress("localhost", 5588);
		SocketSettings settings = SocketSettings.create().withImplReadTimeout(100000L).withImplWriteTimeout(100000L);

		SimpleServer.SocketHandlerProvider socketHandlerProvider = new SimpleServer.SocketHandlerProvider() {
			@Override
			public EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				return new EventHandler() {
					@Override
					public void onRegistered() {
						asyncTcpSocket.read();
					}

					@Override
					public void onRead(ByteBuf buf) {
						eventloop.delay(5, new Runnable() {
							@Override
							public void run() {
								asyncTcpSocket.write(buf);
							}
						});
					}

					@Override
					public void onReadEndOfStream() {
						asyncTcpSocket.close();
					}

					@Override
					public void onWrite() {
						asyncTcpSocket.close();
					}

					@Override
					public void onClosedWithError(Exception e) {
						asyncTcpSocket.close();
					}
				};
			}
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withSocketSettings(settings)
				.withListenAddress(address);

		server.listen();

		eventloop.connect(address).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, settings);
				asyncTcpSocket.setEventHandler(new EventHandler() {
					@Override
					public void onRegistered() {
						asyncTcpSocket.write(ByteBufStrings.wrapAscii("Hello!"));
					}

					@Override
					public void onRead(ByteBuf buf) {
						buf.recycle();
						asyncTcpSocket.close();
						server.close();
					}

					@Override
					public void onReadEndOfStream() {
						asyncTcpSocket.close();
					}

					@Override
					public void onWrite() {
						asyncTcpSocket.read();
					}

					@Override
					public void onClosedWithError(Exception e) {
						asyncTcpSocket.close();
						server.close();
					}
				});
				asyncTcpSocket.register();
			} else {
				throwable.printStackTrace();
				fail();
			}
		});

		eventloop.run();
	}
}