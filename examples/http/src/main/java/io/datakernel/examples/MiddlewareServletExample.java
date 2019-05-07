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

package io.datakernel.examples;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;

import java.io.IOException;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.MediaTypes.HTML;

public class MiddlewareServletExample {

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		RoutingServlet dispatcher = RoutingServlet.create()
				.with(GET, "/", request -> Promise.of(HttpResponse.ok200()
						.withBody(wrapUtf8("<center><h2>Go to some pages:</h2><a href=\"/path1\">Path 1</a><br><a href=\"/path2\">Path 2</a></center>"))
						.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(HTML)))))
				.with(GET, "/path1", request -> Promise.of(HttpResponse.ok200()
						.withBody(wrapUtf8("<center><h2>Hello from the first path!</h2><a href=\"/\">Go home</a></center></center>"))
						.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(HTML)))))
				.with(GET, "/path2", request -> Promise.of(HttpResponse.ok200()
						.withBody(wrapUtf8("<center><h2>Hello from the second path!</h2><a href=\"/\">Go home</a></center></center>"))
						.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(HTML)))))
				.with("/*", request -> Promise.of(HttpResponse.ofCode(404)
						.withBody(wrapUtf8("<center><h1>404</h1><p>Path '" + request.getRelativePath() + "' not found</p><a href=\"/\">Go home</a></center>"))
						.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(HTML)))));

		AsyncHttpServer server = AsyncHttpServer.create(eventloop, dispatcher)
				.withListenPort(8080);

		server.listen();

		System.out.println("Server is running");
		System.out.println("You can connect from browser by visiting 'http://localhost:8080/'");

		eventloop.run();
	}
}
