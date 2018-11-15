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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventStats;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class AbstractHttpConnectionTest {
	private static final int PORT = 5050;
	private static final String url = "http://127.0.0.1:" + PORT;

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	private AsyncHttpClient client = AsyncHttpClient.create(eventloop);

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testMultiLineHeader() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> Promise.of(
						HttpResponse.ok200()
								.withHeader(DATE, "Mon, 27 Jul 2009 12:28:53 GMT")
								.withHeader(CONTENT_TYPE, "text/\n          html")
								.withBody(ByteBufStrings.wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>"))))
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		Map<String, String> data = new HashMap<>();
		CompletableFuture<Void> future = client.request(HttpRequest.get(url))
				.thenCompose(ensureResponseBody())
				.whenResult(result -> {
					data.put("body", result.getBody().asString(UTF_8));
					data.put("header", result.getHeaderOrNull(CONTENT_TYPE));
				})
				.thenCompose($ -> stopClientAndServer(client, server))
				.toCompletableFuture();

		eventloop.run();
		future.get();

		assertEquals("text/           html", data.get("header"));
		assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", data.get("body"));
	}

	@Test
	public void testGzipCompression() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> Promise.of(
						HttpResponse.ok200()
								.withBodyGzipCompression()
								.withBody(encodeAscii("Test message"))))
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		HttpRequest request = HttpRequest.get(url)
				.withHeader(ACCEPT_ENCODING, "gzip");
		CompletableFuture<String> future = client.request(request)
				.thenCompose(ensureResponseBody())
				.thenApply(response -> {
					assertNotNull(response.getHeaderOrNull(CONTENT_ENCODING));
					return response.getBody().asString(UTF_8);
				})
				.whenComplete(($, e) -> stopClientAndServer(client, server))
				.toCompletableFuture();

		eventloop.run();
		assertEquals("Test message", future.get());
	}

	@Test
	public void testClientWithMaxKeepAliveRequests() throws Exception {
		client.withMaxKeepAliveRequests(3);

		AsyncServlet servlet = request -> Promise.of(HttpResponse.ok200());
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		assert client.getStats() != null;
		EventStats clientConnectionCount = client.getStats().getConnected();

		client.request(HttpRequest.get(url))
				.thenCompose(ensureResponseBody())
				.whenResult(response1 -> {
					clientConnectionCount.refresh(System.currentTimeMillis());
					assertEquals("keep-alive", response1.getHeaderOrNull(CONNECTION));
					assertEquals(1, clientConnectionCount.getTotalCount());
					eventloop.post(() -> {
						client.request(HttpRequest.get(url))
								.thenCompose(ensureResponseBody())
								.whenResult(response2 -> {
									clientConnectionCount.refresh(System.currentTimeMillis());
									assertEquals("keep-alive", response2.getHeaderOrNull(CONNECTION));
									assertEquals(1, clientConnectionCount.getTotalCount());
									eventloop.post(() -> {
										client.request(HttpRequest.get(url))
												.thenCompose(ensureResponseBody())
												.whenResult(response3 -> {
													clientConnectionCount.refresh(System.currentTimeMillis());
													assertEquals("close", response3.getHeaderOrNull(CONNECTION));
													assertEquals(1, client.getStats().getConnected().getTotalCount());
													eventloop.post(() -> {
														client.request(HttpRequest.get(url))
																.thenCompose(ensureResponseBody())
																.whenResult(r -> {
																	clientConnectionCount.refresh(System.currentTimeMillis());
																	assertEquals("keep-alive", r.getHeaderOrNull(CONNECTION));
																	assertEquals(2, clientConnectionCount.getTotalCount());
																	stopClientAndServer(client, server);
																});
													});
												});
									});
								});
					});
				});

		eventloop.run();
	}

	@Test
	public void testServerWithMaxKeepAliveRequests() throws Exception {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> Promise.of(
						HttpResponse.ok200()))
				.withListenAddress(new InetSocketAddress("localhost", PORT))
				.withMaxKeepAliveRequests(3);
		server.listen();

		EventStats serverConnectionCount = server.getAccepts();
		assert serverConnectionCount != null;

		client.request(HttpRequest.get(url))
				.thenCompose(ensureResponseBody())
				.whenResult(response1 -> {
					serverConnectionCount.refresh(System.currentTimeMillis());
					assertEquals("keep-alive", response1.getHeaderOrNull(CONNECTION));
					assertEquals(1, serverConnectionCount.getTotalCount());
					eventloop.post(() -> {
						client.request(HttpRequest.get(url))
								.thenCompose(ensureResponseBody())
								.whenResult(response2 -> {
									serverConnectionCount.refresh(System.currentTimeMillis());
									assertEquals("keep-alive", response2.getHeaderOrNull(CONNECTION));
									assertEquals(1, serverConnectionCount.getTotalCount());
									eventloop.post(() -> {
										client.request(HttpRequest.get(url))
												.thenCompose(ensureResponseBody())
												.whenResult(response3 -> {
													serverConnectionCount.refresh(System.currentTimeMillis());
													assertEquals("close", response3.getHeaderOrNull(CONNECTION));
													assertEquals(1, serverConnectionCount.getTotalCount());
													eventloop.post(() -> {
														client.request(HttpRequest.get(url))
																.thenCompose(ensureResponseBody())
																.whenResult(r -> {
																	serverConnectionCount.refresh(System.currentTimeMillis());
																	assertEquals("keep-alive", r.getHeaderOrNull(CONNECTION));
																	assertEquals(2, serverConnectionCount.getTotalCount());
																	stopClientAndServer(client, server);
																});
													});
												});
									});
								});
					});
				});

		eventloop.run();
	}

	private Promise<Void> stopClientAndServer(AsyncHttpClient client, AsyncHttpServer server) {
		return Promises.all(client.stop(), server.close());
	}
}
