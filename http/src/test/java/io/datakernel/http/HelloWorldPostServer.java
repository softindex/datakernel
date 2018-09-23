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

import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.AsyncServlet.ensureBody;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HelloWorldPostServer {
	public static final int PORT = 5588;
	public static final String HELLO_WORLD = "Hello, World!";

	public static AsyncHttpServer helloWorldServer(Eventloop primaryEventloop, int port) {
		return AsyncHttpServer.create(primaryEventloop,
				ensureBody(request -> Stage.of(
						HttpResponse.ok200()
								.withBody(encodeAscii(HELLO_WORLD + request.getBody().asString(UTF_8))))))
				.withListenAddress(new InetSocketAddress("localhost", port));
	}

	public static void main(String[] args) throws Exception {
		Eventloop primaryEventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer httpServerListener = helloWorldServer(primaryEventloop, PORT);

		System.out.println("Start HelloWorld HTTP Server on :" + PORT);
		httpServerListener.listen();

		primaryEventloop.run();
	}

}
