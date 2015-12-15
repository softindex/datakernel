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

package io.datakernel.guice;

import com.google.common.io.Closeables;
import com.google.inject.*;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.PrimaryNioServer;
import io.datakernel.guice.boot.BootModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.service.AsyncServiceCallbacks;
import io.datakernel.service.ServiceGraph;
import io.datakernel.util.ByteBufStrings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import static com.google.common.io.ByteStreams.readFully;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static org.junit.Assert.assertEquals;

public class HelloWorldGuiceTest {
	public static final int PORT = 7583;
	public static final int WORKERS = 4;

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(BootModule.defaultInstance());
		}

		@Provides
		@Singleton
		NioEventloop primaryEventloop() {
			return new NioEventloop();
		}

		@Provides
		@Singleton
		PrimaryNioServer primaryNioServer(NioEventloop primaryEventloop,
		                                  WorkerThreadsPool workerThreadsPool,
		                                  @WorkerThread Provider<AsyncHttpServer> itemProvider) {
			List<AsyncHttpServer> workerHttpServers = workerThreadsPool.getPoolInstances(WORKERS, itemProvider);
			PrimaryNioServer primaryNioServer = PrimaryNioServer.create(primaryEventloop);
			primaryNioServer.workerNioServers(workerHttpServers);
			primaryNioServer.setListenPort(PORT);
			return primaryNioServer;
		}

		@Provides
		@WorkerThread
		NioEventloop workerEventloop() {
			return new NioEventloop();
		}

		@Provides
		@WorkerThread
		AsyncHttpServer workerHttpServer(@WorkerThread NioEventloop eventloop, @WorkerId final int workerId) {
			return new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
				@Override
				public void serveAsync(HttpRequest request,
				                       ResultCallback<HttpResponse> callback) {
					HttpResponse httpResponse = HttpResponse.create(200);
					httpResponse.body(ByteBuf.wrap(ByteBufStrings.encodeAscii("Hello world: worker server #" + workerId)));
					callback.onResult(httpResponse);
				}
			});
		}

	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(new TestModule());
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		Socket socket0 = new Socket(), socket1 = new Socket();
		try {
			AsyncServiceCallbacks.BlockingServiceCallback callback = AsyncServiceCallbacks.withCountDownLatch();
			serviceGraph.start(callback);
			callback.await();

			socket0.connect(new InetSocketAddress(PORT));
			socket1.connect(new InetSocketAddress(PORT));

			for (int i = 0; i < 10; i++) {
				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #0");

				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #0");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #1");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #1");
			}
		} finally {
			AsyncServiceCallbacks.BlockingServiceCallback callback = AsyncServiceCallbacks.withCountDownLatch();
			serviceGraph.stop(callback);
			callback.await();
			Closeables.close(socket0, true);
			Closeables.close(socket1, true);
		}

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	public static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		Assert.assertEquals(expected, decodeAscii(bytes));
	}

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new TestModule());
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			AsyncServiceCallbacks.BlockingServiceCallback callback = AsyncServiceCallbacks.withCountDownLatch();
			serviceGraph.start(callback);
			callback.await();

			System.out.println("Server started, press enter to stop it.");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			br.readLine();
		} finally {
			AsyncServiceCallbacks.BlockingServiceCallback callback = AsyncServiceCallbacks.withCountDownLatch();
			serviceGraph.stop(callback);
			callback.await();
		}
	}

}

