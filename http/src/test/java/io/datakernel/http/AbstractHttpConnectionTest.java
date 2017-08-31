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

import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpHeaders.*;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

public class AbstractHttpConnectionTest {
	private static final int PORT = 5050;
	private static final String url = "http://127.0.0.1:" + PORT;

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private AsyncHttpClient client = AsyncHttpClient.create(eventloop);

	@Test
	public void testMultiLineHeader() throws Exception {
		AsyncServlet servlet = request -> SettableStage.immediateStage(createMultiLineHeaderWithInitialBodySpacesResponse());
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		final Map<String, String> data = new HashMap<>();
		final CompletableFuture<Void> future = client.send(HttpRequest.get(url)).thenCompose(result -> {
			data.put("body", decodeAscii(result.getBody()));
			data.put("header", result.getHeader(CONTENT_TYPE));
			return stopClientAndServer(client, server);
		}).toCompletableFuture();

		eventloop.run();
		future.get();

		assertEquals("text/           html", data.get("header"));
		assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", data.get("body"));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private HttpResponse createMultiLineHeaderWithInitialBodySpacesResponse() {
		HttpResponse response = HttpResponse.ok200();
		response.addHeader(DATE, "Mon, 27 Jul 2009 12:28:53 GMT");
		response.addHeader(CONTENT_TYPE, "text/\n          html");
		response.setBody(ByteBufStrings.wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>"));
		return response;
	}

	@Test
	public void testGzipCompression() throws Exception {
		AsyncServlet servlet = new AsyncServlet() {
			boolean first = true;

			@Override
			public CompletionStage<HttpResponse> serve(HttpRequest request) {
				HttpResponse response = HttpResponse.ok200().withBodyGzipCompression();
				if (!first) {
					return SettableStage.immediateStage(response.withBody((ByteBuf) null));
				} else {
					first = false;
					return SettableStage.immediateStage(response.withBody(encodeAscii("Test message")));
				}
			}
		};
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		final HttpRequest request = HttpRequest.get(url).withHeader(ACCEPT_ENCODING, "gzip");
		final CompletableFuture<Void> future = client.send(request).thenCompose(response -> {
			assertNotNull(response.getHeaderValue(CONTENT_ENCODING));
			return client.send(HttpRequest.get(url)).thenCompose(innerResponse -> {
				assertNull(innerResponse.getHeaderValue(CONTENT_ENCODING));
				return stopClientAndServer(client, server);
			});
		}).toCompletableFuture();

		eventloop.run();
		future.get();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private CompletionStage<Void> stopClientAndServer(final AsyncHttpClient client, final AsyncHttpServer server) {
		return client.stop().runAfterBoth(server.close(), () -> {
		});
	}
}