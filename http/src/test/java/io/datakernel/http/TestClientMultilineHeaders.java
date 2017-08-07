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

import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertEquals;

public class TestClientMultilineHeaders {

	public static final int PORT = 9595;
	public static final InetAddress GOOGLE_PUBLIC_DNS = HttpUtils.inetAddress("8.8.8.8");

	@Test
	public void testMultilineHeaders() throws ExecutionException, InterruptedException, IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse response = HttpResponse.ok200();
				response.addHeader(HttpHeaders.ALLOW, "GET,\r\n HEAD");
				callback.setResult(response);
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.setResult(result.getHeader(HttpHeaders.ALLOW));
				httpClient.stop(IgnoreCompletionCallback.create());
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.setException(exception);
				httpClient.stop(IgnoreCompletionCallback.create());
				server.close(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		assertEquals("GET,   HEAD", resultObserver.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
