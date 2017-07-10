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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Test;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

public class GzipServletTest {
	private static final AsyncServlet helloWorldServlet = new AsyncServlet() {
		@Override
		public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
			callback.setResult(HttpResponse.ok200().withBody(wrapAscii("Hello, World!")));
		}
	};

	@Test
	public void testGzipServletBase() throws Exception {
		GzipServlet gzipServlet = GzipServlet.create(5, helloWorldServlet);
		HttpRequest request = HttpRequest.get("http://example.com")
				.withBody(wrapAscii("Hello, world!"));
		HttpRequest requestGzip = HttpRequest.get("http://example.com")
				.withAcceptGzip().withBody(wrapAscii("Hello, world!"));

		ResultCallbackFuture<HttpResponse> future = ResultCallbackFuture.create();
		gzipServlet.serve(request, future);
		HttpResponse response = future.get();
		assertFalse(response.useGzip);
		response.recycleBufs();

		future = ResultCallbackFuture.create();
		gzipServlet.serve(requestGzip, future);
		response = future.get();
		assertTrue(response.useGzip);
		response.recycleBufs();

		request.recycleBufs();
		requestGzip.recycleBufs();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDoesNotServeSmallBodies() throws Exception {
		AsyncServlet asyncServlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse response = HttpResponse.ok200();
				String requestNum = decodeAscii(request.getBody());
				ByteBuf body = "1".equals(requestNum) ? wrapAscii("0123456789012345678901") : wrapAscii("0");
				callback.setResult(response.withBody(body));
			}
		};
		GzipServlet customGzipServlet = GzipServlet.create(20, asyncServlet);
		HttpRequest requestWBody = HttpRequest.get("http://example.com").withAcceptGzip().withBody(wrapAscii("1"));
		HttpRequest requestWOBody = HttpRequest.get("http://example.com").withAcceptGzip().withBody(wrapAscii("2"));

		ResultCallbackFuture<HttpResponse> future = ResultCallbackFuture.create();
		customGzipServlet.serve(requestWOBody, future);
		HttpResponse response = future.get();
		assertFalse(response.useGzip);
		response.recycleBufs();

		future = ResultCallbackFuture.create();
		customGzipServlet.serve(requestWBody, future);
		response = future.get();
		assertTrue(response.useGzip);
		response.recycleBufs();

		requestWBody.recycleBufs();
		requestWOBody.recycleBufs();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testClientServerIntegration() throws Exception {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, GzipServlet.create(5, helloWorldServlet))
				.withListenPort(1239);
		final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		server.listen();
		AsyncRunnables.runInSequence(eventloop,
				new AsyncRunnable() {
					@Override
					public void run(final CompletionCallback callback) {
						client.send(HttpRequest.get("http://127.0.0.1:1239"), new ForwardingResultCallback<HttpResponse>(callback) {
							@Override
							protected void onResult(HttpResponse result) {
								assertNull(result.getHeader(HttpHeaders.CONTENT_ENCODING));
								callback.setComplete();
							}
						});
					}
				},
				new AsyncRunnable() {
					@Override
					public void run(final CompletionCallback callback) {
						client.send(HttpRequest.get("http://127.0.0.1:1239").withAcceptGzip(), new ForwardingResultCallback<HttpResponse>(callback) {
							@Override
							protected void onResult(HttpResponse result) {
								assertNotNull(result.getHeader(HttpHeaders.CONTENT_ENCODING));
								callback.setComplete();
							}
						});
					}
				}
		).run(new CompletionCallback() {
			@Override
			protected void onComplete() {
				server.close(IgnoreCompletionCallback.create());
				client.stop(IgnoreCompletionCallback.create());
			}

			@Override
			protected void onException(Exception e) {
				e.printStackTrace();
				fail("should not end here");
			}
		});
		eventloop.run();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}