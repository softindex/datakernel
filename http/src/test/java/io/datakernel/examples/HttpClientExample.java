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

import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.Utils;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * Example 2.
 * Example of using asynchronous HTTP client.
 * It is creation of simple HTTP client which sends
 * HTTP GET request with some body and outputs response from server, and running this HTTP client.
 */
public class HttpClientExample {
	private static final int PORT = 5588;
	private static final String CLIENT_NAME = "client";

	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		Eventloop eventloop = new Eventloop();

		// Create the server, to which client will connect
		final AsyncHttpServer httpServer = HttpServerExample.helloWorldServer(eventloop, PORT);

		// Create the client
		final AsyncHttpClient httpClient = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
						3_000L, Utils.forString("8.8.8.8")));
		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		httpServer.listen();

		// Create POST request with body
		HttpRequest request = HttpRequest.post("http://127.0.0.1:" + PORT)
				.body(ByteBuf.wrap(encodeAscii(CLIENT_NAME)));

		// Sending previously formed request
		httpClient.execute(request, 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(decodeAscii(result.getBody()));
				httpClient.close();
				httpServer.close();
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.onException(exception);
				httpClient.close();
				httpServer.close();
			}
		});

		eventloop.run();

		System.out.println("Server response: " + resultObserver.get());
	}
}
