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

package io.datakernel.launchers.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.net.PrimaryServer;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraph;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class HttpWorkerServerTest {
	public static final int PORT = getFreePort();

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws Exception {
		MultithreadedHttpServerLauncher launcher = new MultithreadedHttpServerLauncher() {
			@Provides
			@Worker
			AsyncServlet servlet(@WorkerId int worker) {
				return request -> Promise.of(
						HttpResponse.ok200().withBody(ByteBuf.wrapForReading(encodeAscii("Hello, world! #" + worker))));
			}

			@Override
			protected Module getOverrideModule() {
				return new AbstractModule() {
					@Provides
					Config config() {
						return Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(HttpWorkerServerTest.PORT)));
					}
				};
			}
		};
		Injector injector = launcher.createInjector(new String[]{});
		injector.getInstance(PrimaryServer.class);

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try (Socket socket0 = new Socket(); Socket socket1 = new Socket()) {
			serviceGraph.startFuture().get();

			InetSocketAddress localhost = new InetSocketAddress("localhost", PORT);
			socket0.connect(localhost);
			socket1.connect(localhost);

			for (int i = 0; i < 10; i++) {
				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #0");

				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #0");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #1");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 16\r\n\r\nHello, world! #1");
			}
		} finally {
			serviceGraph.stopFuture().get();
		}
	}

	private static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];

		int length = bytes.length;
		int total = 0;
		while (total < length) {
			int result = is.read(bytes, total, length - total);
			if (result == -1) {
				break;
			}
			total += result;
		}

		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(decodeAscii(bytes).split("\r\n"))));
	}
}

