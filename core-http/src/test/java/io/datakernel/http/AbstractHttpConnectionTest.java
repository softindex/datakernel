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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.MemSize;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelByteChunker;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.jmx.stats.EventStats;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.*;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public final class AbstractHttpConnectionTest {
	private static final int PORT = getFreePort();
	private static final String URL = "http://127.0.0.1:" + PORT;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private static final Random RANDOM = ThreadLocalRandom.current();

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

	@Test
	public void testGzipHugeBuf() throws IOException {
		int size = 1_000_000;
		ByteBuf expected = ByteBufPool.allocate(size);
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> {
					byte[] bytes = new byte[size];
					RANDOM.nextBytes(bytes);
					expected.put(bytes);
					return HttpResponse.ok200()
							.withBodyGzipCompression()
							.withBodyStream(ChannelSupplier.of(expected.slice()));
				})
				.withAcceptOnce()
				.withListenPort(PORT);

		server.listen();

		ByteBuf result = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop()).request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.then(response -> response.getBodyStream().toCollector(ByteBufQueue.collector()))
				.whenComplete(server::close));
		assertArrayEquals(expected.asArray(), result.asArray());
	}

	@Test
	public void testEmptyRequestResponse() {
		List<Consumer<HttpMessage>> messageDecorators = asList(
				message -> {},
				HttpMessage::setBodyGzipCompression,
				message -> message.addHeader(CONTENT_LENGTH, "0"),
				message -> {
					message.setBodyGzipCompression();
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> message.setBody(ByteBuf.empty()),
				message -> {
					message.setBody(ByteBuf.empty());
					message.setBodyGzipCompression();
				},
				message -> {
					message.setBody(ByteBuf.empty());
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> {
					message.setBody(ByteBuf.empty());
					message.setBodyGzipCompression();
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> message.setBodyStream(ChannelSupplier.of()),
				message -> {
					message.setBodyStream(ChannelSupplier.of());
					message.setBodyGzipCompression();
				},
				message -> {
					message.setBodyStream(ChannelSupplier.of());
					message.addHeader(CONTENT_LENGTH, "0");
				},
				message -> {
					message.setBodyStream(ChannelSupplier.of());
					message.setBodyGzipCompression();
					message.addHeader(CONTENT_LENGTH, "0");
				}
		);

		doTestEmptyRequestResponsePermutations(messageDecorators);
	}

	private void doTestEmptyRequestResponsePermutations(List<Consumer<HttpMessage>> messageDecorators) {
		for (int i = 0; i < messageDecorators.size(); i++) {
			for (int j = 0; j < messageDecorators.size(); j++) {
				try {
					Consumer<HttpMessage> responseDecorator = messageDecorators.get(j);
					AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
							$ -> {
								HttpResponse response = HttpResponse.ok200();
								responseDecorator.accept(response);
								return response;
							})
							.withListenPort(PORT)
							.withAcceptOnce(true);

					try {
						server.listen();
					} catch (IOException e) {
						throw new AssertionError(e);
					}

					HttpRequest request = HttpRequest.post(URL);
					messageDecorators.get(i).accept(request);

					String responseText = await(client.request(request)
							.then(HttpMessage::loadBody)
							.map(buf -> buf.getString(UTF_8)));

					assertTrue(responseText.isEmpty());
				} catch (AssertionError e) {
					System.out.println("Error while testing request decorator #" + i + " with response decorator #" + j);
					throw e;
				}
			}
		}
	}

	private static void doTestHugeStreams(AsyncHttpClient client, SocketSettings socketSettings, int size, Consumer<HttpMessage> decorator) throws IOException {
		ByteBuf expected = ByteBufPool.allocate(size);
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(),
				request -> request.loadBody()
						.map(body -> {
							HttpResponse httpResponse = HttpResponse.ok200()
									.withBodyStream(request.getBodyStream()
											.transformWith(ChannelByteChunker.create(MemSize.of(1), MemSize.of(1))));
							decorator.accept(httpResponse);
							return httpResponse;
						}))
				.withListenPort(PORT)
				.withSocketSettings(socketSettings);
		server.listen();

		HttpRequest request = HttpRequest.post("http://127.0.0.1:" + PORT)
				.withBodyStream(ChannelSupplier.ofStream(Stream.generate(() -> {
					byte[] temp = new byte[1];
					RANDOM.nextBytes(temp);
					ByteBuf buf = ByteBuf.wrapForReading(temp);
					expected.put(buf.slice());
					return buf;
				}).limit(size)));

		decorator.accept(request);

		ByteBuf result = await(client.request(request)
				.then(response -> response.getBodyStream()
						.toCollector(ByteBufQueue.collector()))
				.whenComplete(server::close));
		assertArrayEquals(expected.asArray(), result.asArray());
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
