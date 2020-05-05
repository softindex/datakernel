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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;

import java.io.IOException;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpHeaders.ACCEPT_ENCODING;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.test.TestUtils.getFreePort;

public final class GzipCompressingBehaviourExample {
	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		RoutingServlet servlet = RoutingServlet.create()
				// always responds in gzip
				.map(GET, "/gzip/",
						request -> HttpResponse.ok200().withBodyGzipCompression().withBody(encodeAscii("Hello!")))
				// never responds in gzip
				.map(GET, "/nogzip/",
						request -> HttpResponse.ok200().withBody(encodeAscii("Hello!")));

		AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenPort(getFreePort());

		server.listen();
		eventloop.run();

		// this is how you should send an http request with gzipped body.
		// if the content of the response is gzipped - it would be decompressed automatically
		AsyncHttpClient client = AsyncHttpClient.create(eventloop);

		// !sic, you should call withAcceptEncodingGzip for your request if you want to get the response gzipped
		client.request(
				HttpRequest.post("http://example.com")
						.withBody(encodeAscii("Hello, world!"))
						.withBodyGzipCompression()
						.withHeader(ACCEPT_ENCODING, "gzip"));
	}
}
