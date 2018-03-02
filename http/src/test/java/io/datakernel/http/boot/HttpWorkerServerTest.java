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

package io.datakernel.http.boot;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigsModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.launcher.modules.PrimaryEventloopModule;
import io.datakernel.launcher.modules.WorkerEventloopModule;
import io.datakernel.launcher.modules.WorkerPoolModule;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Primary;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import static com.google.inject.Stage.PRODUCTION;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigUtils.initializePrimaryServer;
import static io.datakernel.http.boot.ConfigUtils.initializeHttpWorker;
import static org.junit.Assert.assertEquals;

public class HttpWorkerServerTest {
	public static final int PORT = 7124;

	@Before
	public void before() {
		ByteBufPool.clear();
	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				ConfigsModule.create(Config.create()
						.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT)))),
				WorkerPoolModule.create(),
				PrimaryEventloopModule.create(),
				WorkerEventloopModule.create(),
				new SimpleModule() {
					@Provides
					@Singleton
					public PrimaryServer providePrimaryServer(@Primary Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
						List<AsyncHttpServer> workerHttpServers = workerPool.getInstances(AsyncHttpServer.class);
						return PrimaryServer.create(primaryEventloop, workerHttpServers)
								.initialize(server -> initializePrimaryServer(server, config.getChild("http")));
					}

					@Provides
					@Worker
					public AsyncHttpServer provide(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(server -> initializeHttpWorker(server, config.getChild("http")));
					}
				},
				HelloWorldWorkerServletModule.create());

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

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
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

		assertEquals(expected, decodeAscii(bytes));
	}
}

