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

package io.datakernel;

import io.datakernel.async.IgnoreResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;

import java.io.IOException;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

public final class GzipCompressingBehaviourExample {
	public static void main(String[] args) throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		final MiddlewareServlet dispatcher = MiddlewareServlet.create();

		// serves gzip by default if other side expects - depends on server settings
		dispatcher.with(HttpMethod.GET, "/default/", new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.setResult(HttpResponse.ok200().withBody(encodeAscii("Hello!")));
			}
		});

		// always responds in gzip - if other side requires
		dispatcher.with(HttpMethod.GET, "/gzip/", new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.setResult(HttpResponse.ok200().withBody(encodeAscii("Hello!"), true));
			}
		});

		// never responds in gzip
		dispatcher.with(HttpMethod.GET, "/nogzip/", new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.setResult(HttpResponse.ok200().withBody(encodeAscii("Hello!"), false));
			}
		});

		// enables compression by default, if nothing specified on the response level
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, dispatcher)
				.withDefaultGzipCompression(true)
				.withListenPort(1234);

		server.listen();
		eventloop.run();

		// this is how you should send an http request with gzipped body.
		// if the content of the response is gzipped - it would be decompressed automatically
		final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		// !sic, you should call withAcceptGzip for your request if you want to get the response gzipped
		final HttpRequest request = HttpRequest.post("http://example.com")
				.withBody(encodeAscii("Hello!"), true)
				.withAcceptGzip();

		client.send(request, IgnoreResultCallback.<HttpResponse>create());
	}
}
