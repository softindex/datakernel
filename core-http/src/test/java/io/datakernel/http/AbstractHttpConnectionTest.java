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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventStats;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class AbstractHttpConnectionTest {
	private static final int PORT = getFreePort();
	private static final String URL = "http://127.0.0.1:" + PORT;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private AsyncHttpClient client;

	@Before
	public void setUp() {
		client = AsyncHttpClient.create(Eventloop.getCurrentEventloop()).withInspector(new AsyncHttpClient.JmxInspector());
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

		ByteBuf body = await(client.request(HttpRequest.get(URL))
				.whenComplete(assertComplete(response -> assertEquals("text/           html", response.getHeaderOrNull(CONTENT_TYPE))))
				.then(HttpMessage::getBody));

		assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", body.asString(UTF_8));
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

		ByteBuf body = await(client.request(HttpRequest.get(URL).withHeader(ACCEPT_ENCODING, "gzip"))
				.whenComplete(assertComplete(response -> assertNotNull(response.getHeaderOrNull(CONTENT_ENCODING))))
				.then(HttpMessage::getBody));

		assertEquals("Test message", body.asString(UTF_8));
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

	private Promise<HttpResponse> checkRequest(String expectedHeader, int expectedConnectionCount, EventStats connectionCount) {
		return client.request(HttpRequest.get(URL))
				.then(response -> response.getBody()
						.whenComplete((body, e) -> {
							if (e != null) throw new AssertionError(e);
							try {
								assertEquals(expectedHeader, response.getHeader(CONNECTION));
								connectionCount.refresh(System.currentTimeMillis());
								assertEquals(expectedConnectionCount, connectionCount.getTotalCount());
							} catch (ParseException e1) {
								throw new AssertionError(e1);
							}
						})
						.map($ -> response));
	}

	@SuppressWarnings("SameParameterValue")
	private void checkMaxKeepAlive(int maxKeepAlive, AsyncHttpServer server, EventStats connectionCount) {
		await(Promises.sequence(
				IntStream.range(0, maxKeepAlive - 1)
						.mapToObj($ ->
								() -> checkRequest("keep-alive", 1, connectionCount)
										.post()
										.toVoid()))
				.then($ -> checkRequest("close", 1, connectionCount))
				.post()
				.then($ -> checkRequest("keep-alive", 2, connectionCount))
				.post()
				.whenComplete(($, e) -> server.close()));
	}
}
