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

package io.datakernel.http;

import io.datakernel.async.ResultCallback;
import io.datakernel.async.SimpleCompletionFuture;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.PrimaryNioServer;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.service.NioEventloopRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

import static com.google.common.io.ByteStreams.readFully;
import static com.google.common.io.ByteStreams.toByteArray;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkerServersTest {
	final static int PORT = 9444;
	final static int WORKERS = 4;

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	public static AsyncHttpServer echoServer(NioEventloop primaryEventloop, final int workerN) {
		return new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse content = HttpResponse.create().body(encodeAscii("Hello world: worker #" + workerN));
				callback.onResult(content);
			}

		});
	}

	private void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		Assert.assertEquals(expected, decodeAscii(bytes));
	}

	@Test
	public void simpleWorkers() throws Exception {
		final ArrayList<AsyncHttpServer> workerServers = new ArrayList<>();

		for (int i = 0; i < WORKERS; i++) {
			final NioEventloop eventloop = new NioEventloop();
			eventloop.keepAlive(true);
			AsyncHttpServer server = echoServer(eventloop, i);
			workerServers.add(server);
			new Thread(eventloop).start();
		}

		NioEventloop primaryEventloop = new NioEventloop();
		PrimaryNioServer primaryNioServer = PrimaryNioServer.create(primaryEventloop)
				.workerNioServers(workerServers)
				.setListenPort(PORT);
		primaryNioServer.listen();

		Thread primaryThread = new Thread(primaryEventloop);
		primaryThread.start();

		Socket socket1 = new Socket();
		Socket socket2 = new Socket();
		socket1.connect(new InetSocketAddress(PORT));
		socket2.connect(new InetSocketAddress(PORT));

		socket1.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
		readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 22\r\n\r\nHello world: worker #0");
		socket1.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n"));
		readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\nHello world: worker #0");
		assertTrue(toByteArray(socket1.getInputStream()).length == 0);
		socket1.close();

		socket2.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
		readAndAssert(socket2.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 22\r\n\r\nHello world: worker #1");
		socket2.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n"));
		readAndAssert(socket2.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\nHello world: worker #1");
		assertTrue(toByteArray(socket2.getInputStream()).length == 0);
		socket2.close();


		SimpleCompletionFuture callbackPrimaty = new SimpleCompletionFuture();
		primaryNioServer.closeFuture(callbackPrimaty);
		callbackPrimaty.await();

		primaryThread.join();
		for (AsyncHttpServer server : workerServers) {
			server.getNioEventloop().keepAlive(false);
			SimpleCompletionFuture callbackServer = new SimpleCompletionFuture();
			server.closeFuture(callbackServer);
			callbackServer.await();
		}

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void serviceRunner() throws Exception {
		ArrayList<NioEventloopRunner> workerServices = new ArrayList<>();
		ArrayList<AsyncHttpServer> workerServers = new ArrayList<>();

		for (int i = 0; i < WORKERS; i++) {
			NioEventloop workerEventloop = new NioEventloop();
			AsyncHttpServer workerServer = echoServer(workerEventloop, i);
			workerServers.add(workerServer);

			workerServices.add(new NioEventloopRunner(workerEventloop)
					.addNioServers(workerServer));
		}

		NioEventloop primaryEventloop = new NioEventloop();
		PrimaryNioServer primaryNioServer = PrimaryNioServer.create(primaryEventloop)
				.workerNioServers(workerServers)
				.setListenPort(PORT);

		NioEventloopRunner primaryService = new NioEventloopRunner(primaryEventloop)
				.addNioServers(primaryNioServer)
				.addConcurrentServices(workerServices);

		SimpleCompletionFuture callback = new SimpleCompletionFuture();
		primaryService.startFuture(callback);
		callback.await();

		Socket socket1 = new Socket();
		Socket socket2 = new Socket();
		socket1.connect(new InetSocketAddress(PORT));
		socket2.connect(new InetSocketAddress(PORT));

		socket1.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
		readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 22\r\n\r\nHello world: worker #0");
		socket1.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n"));
		readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\nHello world: worker #0");
		assertTrue(toByteArray(socket1.getInputStream()).length == 0);
		socket1.close();

		socket2.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
		readAndAssert(socket2.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 22\r\n\r\nHello world: worker #1");
		socket2.getOutputStream().write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n"));
		readAndAssert(socket2.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 22\r\n\r\nHello world: worker #1");
		assertTrue(toByteArray(socket2.getInputStream()).length == 0);
		socket2.close();

		SimpleCompletionFuture callbackStop = new SimpleCompletionFuture();
		primaryService.stopFuture(callbackStop);
		callbackStop.await();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}
