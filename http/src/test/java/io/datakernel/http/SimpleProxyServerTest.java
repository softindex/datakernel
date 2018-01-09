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

import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.dns.AsyncDnsClient;
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
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
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

	public static AsyncHttpServer proxyHttpServer(Eventloop primaryEventloop, AsyncHttpClient httpClient) {
		AsyncServlet servlet = request -> {
			String path = ECHO_SERVER_PORT + request.getUrl().getPath();
			return httpClient.send(HttpRequest.get("http://127.0.0.1:" + path)).thenApply(result -> {
				int code = result.getCode();
				byte[] body = encodeAscii("FORWARDED: " + decodeAscii(result.getBody()));
				return HttpResponse.ofCode(code).withBody(body);
			});
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenAddress(new InetSocketAddress("localhost", PROXY_SERVER_PORT));
	}

	public static AsyncHttpServer echoServer(Eventloop primaryEventloop) {
		AsyncServlet servlet = request -> Stages.of(HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery())));

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenAddress(new InetSocketAddress("localhost", ECHO_SERVER_PORT));
	}

	private void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		Assert.assertEquals(expected, decodeAscii(bytes));
	}

	@Test
	public void testSimpleProxyServer() throws Exception {
		Eventloop eventloop1 = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		AsyncHttpServer echoServer = echoServer(eventloop1);
		echoServer.listen();
		Thread echoServerThread = new Thread(eventloop1);
		echoServerThread.start();

		Eventloop eventloop2 = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		AsyncDnsClient dnsClient = AsyncDnsClient.create(eventloop2)
				.withDatagramSocketSetting(DatagramSocketSettings.create())
				.withDnsServerAddress(HttpUtils.inetAddress("8.8.8.8"));
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop2).withDnsClient(dnsClient);

		AsyncHttpServer proxyServer = proxyHttpServer(eventloop2, httpClient);
		proxyServer.listen();
		Thread proxyServerThread = new Thread(eventloop2);
		proxyServerThread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", PROXY_SERVER_PORT));
		OutputStream stream = socket.getOutputStream();

		stream.write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 15\r\n\r\nFORWARDED: /abc");
		stream.write(encodeAscii("GET /hello HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n"));
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 17\r\n\r\nFORWARDED: /hello");

		httpClient.getEventloop().execute(httpClient::stop);

		echoServer.closeFuture().get();

		proxyServer.closeFuture().get();

		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		socket.close();

		echoServerThread.join();
		proxyServerThread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}
