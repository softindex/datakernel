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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.DatagramSocketSettings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleProxyServerTest {
	final static int ECHO_SERVER_PORT = 9707;
	final static int PROXY_SERVER_PORT = 9444;

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	public static AsyncHttpServer proxyHttpServer(final Eventloop primaryEventloop, final AsyncHttpClient httpClient) {
		return new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final Callback callback) {
				httpClient.send(HttpRequest.get("http://127.0.0.1:" + ECHO_SERVER_PORT + request.getUrl().getPath()), 1000, new ResultCallback<HttpResponse>() {
					@Override
					public void onResult(final HttpResponse result) {
						HttpResponse res = HttpResponse.create(result.getCode());
						res.body(encodeAscii("FORWARDED: " + decodeAscii(result.getBody())));
						callback.onResult(res);
					}

					@Override
					public void onException(Exception exception) {
						callback.onHttpError(new HttpServletError(500, exception));
					}
				});
			}
		});
	}

	public static AsyncHttpServer echoServer(Eventloop primaryEventloop) {
		return new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				HttpResponse content = HttpResponse.create().body(encodeAscii(request.getUrl().getPathAndQuery()));
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
	public void testSimpleProxyServer() throws Exception {
		Eventloop eventloop1 = new Eventloop();
		AsyncHttpServer echoServer = echoServer(eventloop1);
		echoServer.setListenPort(ECHO_SERVER_PORT);
		echoServer.listen();
		Thread echoServerThread = new Thread(eventloop1);
		echoServerThread.start();

		Eventloop eventloop2 = new Eventloop();
		AsyncHttpClient httpClient = new AsyncHttpClient(eventloop2,
				new NativeDnsResolver(eventloop2, new DatagramSocketSettings(), 3_000L, HttpUtils.inetAddress("8.8.8.8")));

		AsyncHttpServer proxyServer = proxyHttpServer(eventloop2, httpClient);
		proxyServer.setListenPort(PROXY_SERVER_PORT).acceptOnce(false);
		proxyServer.listen();
		Thread proxyServerThread = new Thread(eventloop2);
		proxyServerThread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress(PROXY_SERVER_PORT));
		OutputStream stream = socket.getOutputStream();

		stream.write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 15\r\n\r\nFORWARDED: /abc");
		stream.write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n"));
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\nFORWARDED: /hello");

		httpClient.closeFuture().await();

		echoServer.closeFuture().await();

		proxyServer.closeFuture().await();

		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		socket.close();

		echoServerThread.join();
		proxyServerThread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}
