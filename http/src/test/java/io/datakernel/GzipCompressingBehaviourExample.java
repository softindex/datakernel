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

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;

import java.io.IOException;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

public final class GzipCompressingBehaviourExample {
	public static void main(String[] args) throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		final MiddlewareServlet dispatcher = MiddlewareServlet.create();

		// always responds in gzip
		dispatcher.with(HttpMethod.GET, "/gzip/", request -> SettableStage.immediateStage(
				HttpResponse.ok200().withBodyGzipCompression().withBody(encodeAscii("Hello!"))));

		// never responds in gzip
		dispatcher.with(HttpMethod.GET, "/nogzip/", reques -> SettableStage.immediateStage(
				HttpResponse.ok200().withBody(encodeAscii("Hello!"))));

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, dispatcher).withListenPort(1234);

		server.listen();
		eventloop.run();

		// this is how you should send an http request with gzipped body.
		// if the content of the response is gzipped - it would be decompressed automatically
		final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		// !sic, you should call withAcceptEncodingGzip for your request if you want to get the response gzipped
		final HttpRequest request = HttpRequest.post("http://example.com")
				.withBody(encodeAscii("Hello, world!"))
				.withBodyGzipCompression()
				.withAcceptEncodingGzip();

		client.send(request);
	}
}
