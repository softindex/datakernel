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

package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.dns.CachedAsyncDnsClient;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class SimpleProxyServerTest {
	private static final int ECHO_SERVER_PORT = 9707;
	private static final int PROXY_SERVER_PORT = 9444;

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	private void readAndAssert(InputStream is, String expected) {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		String actual = decodeAscii(bytes);
		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(actual.split("\r\n"))));
	}

	@Test
	public void testSimpleProxyServer() throws Exception {
		Eventloop eventloop1 = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer echoServer = AsyncHttpServer.create(eventloop1, request ->
				Promise.of(HttpResponse.ok200()
						.withBody(encodeAscii(request.getUrl().getPathAndQuery()))))
				.withListenPort(ECHO_SERVER_PORT);
		echoServer.listen();

		Thread echoServerThread = new Thread(eventloop1);
		echoServerThread.start();

		Eventloop eventloop2 = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop2)
				.withDnsClient(CachedAsyncDnsClient.create(eventloop2, RemoteAsyncDnsClient.create(eventloop2)
						.withDatagramSocketSetting(DatagramSocketSettings.create())
						.withDnsServerAddress(HttpUtils.inetAddress("8.8.8.8"))));

		AsyncHttpServer proxyServer = AsyncHttpServer.create(eventloop2,
				request -> {
					String path = ECHO_SERVER_PORT + request.getUrl().getPath();
					return httpClient.request(HttpRequest.get("http://127.0.0.1:" + path))
							.then(result -> result.getBody()
									.map(body ->
											HttpResponse.ofCode(result.getCode())
													.withBody(encodeAscii("FORWARDED: " + body.asString(UTF_8)))));
				})
				.withListenPort(PROXY_SERVER_PORT);
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

		assertEquals(0, toByteArray(socket.getInputStream()).length);
		socket.close();

		echoServerThread.join();
		proxyServerThread.join();
	}
}
