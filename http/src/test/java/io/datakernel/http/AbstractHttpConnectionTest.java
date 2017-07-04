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

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.async.AsyncRunnables.runInParallel;
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
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.setResult(createMultiLineHeaderWithInitialBodySpacesResponse());
			}
		};
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		final Map<String, String> data = new HashMap<>();
		client.send(HttpRequest.get(url), new ForwardingResultCallback<HttpResponse>(future) {
			@Override
			public void onResult(HttpResponse result) {
				data.put("body", decodeAscii(result.getBody()));
				data.put("header", result.getHeader(CONTENT_TYPE));
				stopClientAndServer(client, server, future);
			}
		});

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
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse response = HttpResponse.ok200();
				if (!first) {
					response.withBody((ByteBuf) null);
					callback.setResult(response);
					assertNull(response.getHeaderValue(CONTENT_ENCODING));
				} else {
					first = false;
					response.withBody(encodeAscii("Test message"));
					callback.setResult(response);
					assertNotNull(response.getHeaderValue(CONTENT_ENCODING));
				}
			}
		};
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withDefaultGzipCompression(true)
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		final CompletionCallbackFuture future = CompletionCallbackFuture.create();

		client.send(HttpRequest.get(url).withHeader(ACCEPT_ENCODING, "gzip"),
				new ForwardingResultCallback<HttpResponse>(future) {
					@Override
					protected void onResult(HttpResponse result) {
						assertNotNull(result.getHeaderValue(CONTENT_ENCODING));
						client.send(HttpRequest.get(url), new ForwardingResultCallback<HttpResponse>(future) {
							@Override
							protected void onResult(HttpResponse result) {
								assertNull(result.getHeaderValue(CONTENT_ENCODING));
								stopClientAndServer(client, server, future);
							}
						});
					}
				}
		);

		eventloop.run();
		future.get();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private void stopClientAndServer(final AsyncHttpClient client, final AsyncHttpServer server,
	                                 CompletionCallbackFuture callback) {
		runInParallel(
				eventloop,
				new AsyncRunnable() {
					@Override
					public void run(CompletionCallback callback) {
						server.close(callback);
					}
				},
				new AsyncRunnable() {
					@Override
					public void run(CompletionCallback callback) {
						client.stop(callback);
					}
				})
				.run(callback);
	}
}