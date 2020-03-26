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

import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.common.MemSize;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelByteChunker;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.*;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.promise.TestUtils.await;
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
				request -> HttpResponse.ok200()
						.withHeader(DATE, "Mon, 27 Jul 2009 12:28:53 GMT")
						.withHeader(CONTENT_TYPE, "text/\n          html")
						.withBody(wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>")))
				.withListenPort(PORT)
				.withAcceptOnce();
		server.listen();

		await(client.request(HttpRequest.get(URL))
				.then(response -> response.loadBody()
						.whenComplete(assertComplete(body -> {
							assertEquals("text/           html", response.getHeader(CONTENT_TYPE));
							assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", body.getString(UTF_8));
						}))));
	}

	@Test
	public void testGzipCompression() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> HttpResponse.ok200()
						.withBodyGzipCompression()
						.withBody(encodeAscii("Test message")))
				.withListenPort(PORT)
				.withAcceptOnce();

		server.listen();

		await(client.request(HttpRequest.get(URL)
				.withHeader(ACCEPT_ENCODING, "gzip"))
				.then(response -> response.loadBody()
						.whenComplete(assertComplete(body -> {
							assertEquals("Test message", body.getString(UTF_8));
							assertNotNull(response.getHeader(CONTENT_ENCODING));
						}))));
	}

	@Test
	public void testClientWithMaxKeepAliveRequests() throws Exception {
		client.withKeepAliveTimeout(Duration.ofSeconds(1));
		client.withMaxKeepAliveRequests(5);

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> HttpResponse.ok200())
				.withListenPort(PORT);
		server.listen();

		assertNotNull(client.getStats());
		checkMaxKeepAlive(5, server, client.getStats().getConnected());
	}

	@Test
	public void testServerWithMaxKeepAliveRequests() throws Exception {
		client.withKeepAliveTimeout(Duration.ofSeconds(1));

		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request -> HttpResponse.ok200())
				.withListenPort(PORT)
				.withMaxKeepAliveRequests(5);
		server.listen();

		assertNotNull(server.getAccepts());
		checkMaxKeepAlive(5, server, server.getAccepts());
	}

	@Test
	@Ignore("Takes a long time")
	public void testHugeBodyStreams() throws IOException {
		int size = 10_000;

		SocketSettings socketSettings = SocketSettings.create()
				.withSendBufferSize(MemSize.of(1))
				.withReceiveBufferSize(MemSize.of(1))
				.withImplReadBufferSize(MemSize.of(1));

		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withSocketSettings(socketSettings);

		// regular
		doTestHugeStreams(client, socketSettings, size, httpMessage -> httpMessage.addHeader(CONTENT_LENGTH, String.valueOf(size)));

		// chunked
		doTestHugeStreams(client, socketSettings, size, httpMessage -> {});

		// gzipped + chunked
		doTestHugeStreams(client, socketSettings, size, HttpMessage::setBodyGzipCompression);
	}

	private static void doTestHugeStreams(AsyncHttpClient client, SocketSettings socketSettings, int size, Consumer<HttpMessage> decorator) throws IOException {
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> {
					HttpResponse httpResponse = HttpResponse.ok200().withBodyStream(request.getBodyStream());
					decorator.accept(httpResponse);
					return httpResponse;
				})
				.withListenPort(PORT)
				.withSocketSettings(socketSettings);
		server.listen();

		HttpRequest request = HttpRequest.post("http://127.0.0.1:" + PORT)
				.withBodyStream(ChannelSupplier.ofStream(Stream.generate(() -> ByteBufStrings.wrapAscii("a")).limit(size)));

		decorator.accept(request);

		String result = await(client.request(request)
				.then(response -> response.getBodyStream()
						.transformWith(ChannelByteChunker.create(MemSize.of(1), MemSize.of(1)))
						.<CharSequence>map(ByteBufStrings::asAscii)
						.toCollector(Collectors.joining()))
				.whenComplete(server::close));

		assertEquals(size, result.length());
		for (char c : result.toCharArray()) {
			assertEquals('a', c);
		}
	}

	private Promise<HttpResponse> checkRequest(String expectedHeader, int expectedConnectionCount, EventStats connectionCount) {
		return client.request(HttpRequest.get(URL))
				.thenEx((response, e) -> {
					if (e != null) throw new AssertionError(e);
					assertEquals(expectedHeader, response.getHeader(CONNECTION));
					connectionCount.refresh(System.currentTimeMillis());
					assertEquals(expectedConnectionCount, connectionCount.getTotalCount());
					return Promise.of(response);
				});
	}

	@SuppressWarnings("SameParameterValue")
	private void checkMaxKeepAlive(int maxKeepAlive, AsyncHttpServer server, EventStats connectionCount) {
		await(Promises.sequence(
				IntStream.range(0, maxKeepAlive - 1)
						.mapToObj($ ->
								() -> checkRequest("keep-alive", 1, connectionCount)
										.post()
										.toVoid()))
				.then(() -> checkRequest("close", 1, connectionCount))
				.post()
				.then(() -> checkRequest("keep-alive", 2, connectionCount))
				.post()
				.whenComplete(server::close));
	}
}
