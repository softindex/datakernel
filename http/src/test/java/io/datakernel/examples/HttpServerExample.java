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

package io.datakernel.examples;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncHttpServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;

import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * Example 1.
 * Example of using simple asynchronous HTTP server which can handle requests and send response.
 */
public class HttpServerExample {
	public static final int PORT = 5588;
	public static final String HELLO = "Hello ";

	/* Create HTTP server, which responds to each request with the concatenation of "Hello " and request body. */
	public static AsyncHttpServer helloWorldServer(Eventloop primaryEventloop, int port) {
		AsyncHttpServer httpServer = new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				String s = HELLO + decodeAscii(request.getBody());
				HttpResponse content = HttpResponse.create().body(encodeAscii(s));
				callback.onResult(content);
			}
		});
		return httpServer.setListenPort(port);
	}

	/* Run HTTP server in an event loop. */
	public static void main(String[] args) throws Exception {
		final Eventloop primaryEventloop = new Eventloop();
		final AsyncHttpServer httpServerListener = helloWorldServer(primaryEventloop, PORT);

		System.out.println("Started HelloWorld HTTP Server on http://localhost:" + PORT);
		httpServerListener.listen();

		primaryEventloop.run();
	}
}