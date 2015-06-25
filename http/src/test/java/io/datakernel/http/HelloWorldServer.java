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

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.server.AsyncHttpServlet;

import static io.datakernel.util.ByteBufStrings.encodeAscii;

public final class HelloWorldServer {
	public static final int PORT = 5588;
	public static final byte[] HELLO_WORLD = encodeAscii("Hello, World!");

	public static AsyncHttpServer helloWorldServer(NioEventloop primaryEventloop, int port) {
		AsyncHttpServer httpServer = new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse content = HttpResponse.create().body(HELLO_WORLD);
				callback.onResult(content);
			}
		});
		return httpServer.setListenPort(port).acceptOnce(false);
	}

	public static void main(String[] args) throws Exception {
		NioEventloop eventloop = new NioEventloop();

		AsyncHttpServer server = helloWorldServer(eventloop, PORT);

		System.out.println("Start HelloWorld HTTP Server on :" + PORT);
		server.listen();

		eventloop.run();
	}

}