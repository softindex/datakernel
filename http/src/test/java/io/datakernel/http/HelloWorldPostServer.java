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

import io.datakernel.eventloop.Eventloop;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

public final class HelloWorldPostServer {
	public static final int PORT = 5588;
	public static final String HELLO_WORLD = "Hello, World!";

	public static AsyncHttpServer helloWorldServer(Eventloop primaryEventloop, int port) {
		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) {
				String s = HELLO_WORLD + decodeAscii(request.getBody());
				HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(s));
				callback.onResult(content);
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenPort(port);
	}

	public static void main(String[] args) throws Exception {
		final Eventloop primaryEventloop = Eventloop.create();

		final AsyncHttpServer httpServerListener = helloWorldServer(primaryEventloop, PORT);

		System.out.println("Start HelloWorld HTTP Server on :" + PORT);
		httpServerListener.listen();

		primaryEventloop.run();
	}

}