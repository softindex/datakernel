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
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventStats;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.stream.IntStream;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(DatakernelRunner.class)
public final class AbstractHttpConnectionTest {
	private static final int PORT = 5050;
	private static final String URL = "http://127.0.0.1:" + PORT;

	private AsyncHttpClient client;

	@Before
	public void setUp() {
		client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());
	}

	@Test
	public void testMultiLineHeader() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> Promise.of(
						HttpResponse.ok200()
								.withHeader(DATE, "Mon, 27 Jul 2009 12:28:53 GMT")
								.withHeader(CONTENT_TYPE, "text/\n          html")
								.withBody(ByteBufStrings.wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>"))))
				.withListenPort(PORT)
				.withAcceptOnce();
		server.listen();

		client.request(HttpRequest.get(URL))
				.thenCompose(ensureResponseBody())
				.whenComplete(assertComplete(result -> {
					assertEquals("text/           html", result.getHeaderOrNull(CONTENT_TYPE));
					assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", result.getBody().asString(UTF_8));
				}));
	}

	@Test
	public void testGzipCompression() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> Promise.of(
						HttpResponse.ok200()
								.withBodyGzipCompression()
								.withBody(encodeAscii("Test message"))))
				.withListenPort(PORT)
				.withAcceptOnce();

		server.listen();

		client.request(HttpRequest.get(URL).withHeader(ACCEPT_ENCODING, "gzip"))
				.thenCompose(ensureResponseBody())
				.whenComplete(assertComplete(response -> {
					assertNotNull(response.getHeaderOrNull(CONTENT_ENCODING));
					assertEquals("Test message", response.getBody().asString(UTF_8));
				}));
	}

	@Test
	public void testClientWithMaxKeepAliveRequests() throws Exception {
		client.withMaxKeepAliveRequests(5);

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> Promise.of(HttpResponse.ok200()))
				.withListenPort(PORT);
		server.listen();

		assertNotNull(client.getStats());
		checkMaxKeepAlive(5, server, client.getStats().getConnected());
	}

	@Test
	public void testServerWithMaxKeepAliveRequests() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> Promise.of(HttpResponse.ok200()))
				.withListenPort(PORT)
				.withMaxKeepAliveRequests(5);
		server.listen();

		assertNotNull(server.getAccepts());
		checkMaxKeepAlive(5, server, server.getAccepts());
	}

	private Promise<?> checkRequest(String expectedHeader, int expectedConnectionCount, EventStats connectionCount) {
		return client.request(HttpRequest.get(URL))
				.thenCompose(ensureResponseBody())
				.whenComplete(assertComplete(response -> {
					assertEquals(expectedHeader, response.getHeader(CONNECTION));
					connectionCount.refresh(System.currentTimeMillis());
					assertEquals(expectedConnectionCount, connectionCount.getTotalCount());
					System.out.println(response.getHeader(CONNECTION));
				}));
	}

	@SuppressWarnings("SameParameterValue")
	private void checkMaxKeepAlive(int maxKeepAlive, AsyncHttpServer server, EventStats connectionCount) {
		Promises.runSequence(IntStream.range(0, maxKeepAlive - 1)
				.mapToObj($ -> checkRequest("keep-alive", 1, connectionCount).post()))
				.thenCompose($ -> checkRequest("close", 1, connectionCount))
				.post()
				.thenCompose($ -> checkRequest("keep-alive", 2, connectionCount))
				.post()
				.whenComplete(($, e) -> server.close());
	}
}
