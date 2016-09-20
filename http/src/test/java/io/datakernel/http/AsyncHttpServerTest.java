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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
import static java.lang.Math.min;
import static org.junit.Assert.*;

public class AsyncHttpServerTest {

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	public static AsyncHttpServer blockingHttpServer(Eventloop primaryEventloop, int port) {
		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				callback.onResult(content);
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenPort(port);
	}

	public static AsyncHttpServer asyncHttpServer(final Eventloop primaryEventloop, int port) {
		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(final HttpRequest request, final Callback callback) {
				final HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				primaryEventloop.post(new Runnable() {
					@Override
					public void run() {
						callback.onResult(content);
					}
				});
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenPort(port);
	}

	public static AsyncHttpServer delayedHttpServer(final Eventloop primaryEventloop, int port) {
		final Random random = new Random();
		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(final HttpRequest request, final Callback callback) {
				final HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				primaryEventloop.schedule(primaryEventloop.currentTimeMillis() + random.nextInt(3), new Runnable() {
					@Override
					public void run() {
						callback.onResult(content);
					}
				});
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenPort(port);
	}

	public static void writeByRandomParts(Socket socket, String string) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(string));
		Random random = new Random();
		while (buf.canRead()) {
			int count = min(1 + random.nextInt(5), buf.headRemaining());
			socket.getOutputStream().write(buf.array(), buf.head(), count);
			buf.moveHead(count);
		}
	}

	public static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		Assert.assertEquals(expected, decodeAscii(bytes));
	}

	private void doTestKeepAlive(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(new InetSocketAddress(port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");

			writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		}

		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n/abc"); // ?

		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().await();
		thread.join();
	}

	@Test
	public void testKeepAlive() throws Exception {
		Eventloop eventloop = Eventloop.create();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);

		doTestKeepAlive(eventloop, blockingHttpServer(eventloop, port), port);
		doTestKeepAlive(eventloop, asyncHttpServer(eventloop, port), port);
		doTestKeepAlive(eventloop, delayedHttpServer(eventloop, port), port);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testClosed() throws Exception {
		Eventloop eventloop = Eventloop.create();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress(port));
		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\n");
		socket.close();

		server.closeFuture().await();
		thread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testNoKeepAlive() throws Exception {
		Eventloop eventloop = Eventloop.create();

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress(port));
		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n/abc");
		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		socket.close();

		server.closeFuture().await();
		thread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testPipelining() throws Exception {
		Eventloop eventloop = Eventloop.create();
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
//		doTestPipelining(eventloop, blockingHttpServer(eventloop));
//		doTestPipelining(eventloop, asyncHttpServer(eventloop));
		doTestPipelining(eventloop, delayedHttpServer(eventloop, port), port);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	private void doTestPipelining(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress(port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP1.1\r\nConnection: Keep-Alive\r\nHost: localhost\r\n\r\n"
					+ "GET /123456 HTTP1.1\r\nConnection: Keep-Alive\r\nHost: localhost\r\n\r\n");
		}

		for (int i = 0; i < 100; i++) {
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 7\r\n\r\n/123456");
		}

		server.closeFuture().await();
		thread.join();
	}

	@Test
	public void testBigHttpMessage() throws Exception {
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		final Eventloop eventloop = Eventloop.create();
		final ByteBuf buf =
				HttpRequest.post("http://127.0.0.1:" + port)
						.withBody(ByteBuf.wrapForReading(encodeAscii("Test big HTTP message body")))
						.write();

		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(final HttpRequest request, final Callback callback) {
				final HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						callback.onResult(content);
					}
				});
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withMaxHttpMessageSize(25)
				.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(port));
			socket.getOutputStream().write(buf.array(), buf.head(), buf.headRemaining());
			buf.recycle();
			Thread.sleep(100);
		}
		server.closeFuture().await();
		thread.join();
		assertEquals(1, eventloop.getStats().getErrorStats().getIoErrors().getTotal());
		assertEquals("Too big HttpMessage",
				eventloop.getStats().getErrorStats().getIoErrors().getLastException().getMessage());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create();
		AsyncHttpServer server = blockingHttpServer(eventloop, 8888);
		server.listen();
		eventloop.run();
	}
}
