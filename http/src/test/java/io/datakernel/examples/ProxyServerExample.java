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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;

import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * Example 3.
 * Example of creating a simple HTTP proxy-server.
 */
public class ProxyServerExample {
	final static String REDIRECT_ADDRESS = "http://127.0.0.1:" + 5588;

	/* Creates a simple proxy server, which redirects all requests to REDIRECT_ADDRESS. */
	public static AsyncHttpServer createProxyHttpServer(final NioEventloop primaryEventloop,
	                                                    final AsyncHttpClient httpClient) {
		return new AsyncHttpServer(primaryEventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				httpClient.getHttpResultAsync(HttpRequest.get(REDIRECT_ADDRESS + request.getUrl().getPath()), 1000,
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
}
