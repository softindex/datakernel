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
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.util.Utils;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * Example 3.
 * Example of creating a simple HTTP proxy-server.
 */
public class ProxyServerExample {
	public static final int REDIRECT_PORT = 5588;
	public static final int PROXY_PORT = 5587;
	final static String REDIRECT_ADDRESS = "http://127.0.0.1:" + REDIRECT_PORT;

	public static final int TIMEOUT = 1000;

	//  Creates a simple proxy server, which redirects all requests to REDIRECT_ADDRESS.
	public static AsyncHttpServer createProxyHttpServer(final Eventloop eventloop,
	                                                    final AsyncHttpClient client) {
		return new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				client.execute(HttpRequest.get(REDIRECT_ADDRESS + request.getUrl().getPath()), 1000,
						new ResultCallback<HttpResponse>() {
							@Override
							public void onResult(final HttpResponse result) {
								HttpResponse res = HttpResponse.create(result.getCode());
								res.body(encodeAscii("FORWARDED: " + decodeAscii(result.getBody())));
								callback.onResult(res);
							}

							@Override
							public void onException(Exception exception) {
								callback.onException(exception);
							}
						});
			}
		});
	}

	private static AsyncHttpClient createClient(Eventloop eventloop) {
		return new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
						3_000L, Utils.forString("8.8.8.8")));
	}

	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		Eventloop eventloop = new Eventloop();

		// Setting up proxy server
		final AsyncHttpClient proxyClient = createClient(eventloop);
		final AsyncHttpServer proxy = createProxyHttpServer(eventloop, proxyClient)
				.setListenPort(PROXY_PORT);

		// Setting up http server
		final AsyncHttpServer httpServer = HttpServerExample.helloWorldServer(eventloop, REDIRECT_PORT);

		// Setting up client
		final AsyncHttpClient httpClient = createClient(eventloop);

		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		httpServer.listen();
		proxy.listen();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PROXY_PORT);

		// Sending previously formed request
		httpClient.execute(request, TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(decodeAscii(result.getBody()));
				httpClient.close();
				httpServer.close();
				proxy.close();
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.onException(exception);
				httpClient.close();
				httpServer.close();
				proxy.close();
			}
		});

		eventloop.run();

		System.out.println("Server response: " + resultObserver.get());
	}
}
