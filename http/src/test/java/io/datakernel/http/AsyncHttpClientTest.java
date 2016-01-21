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
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static io.datakernel.util.ByteBufStrings.decodeUTF8;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static org.junit.Assert.assertEquals;

public class AsyncHttpClientTest {
	private static final int PORT = 45788;
	public static final byte[] TIMEOUT_EXCEPTION_BYTES = encodeAscii("ERROR: Must be TimeoutException");

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testAsyncClient() throws Exception {
		final Eventloop eventloop = new Eventloop();

		final AsyncHttpServer httpServer = HelloWorldServer.helloWorldServer(eventloop, PORT);
		final AsyncHttpClient httpClient = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, 3_000L, Utils.forString("8.8.8.8")));
		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		httpServer.listen();

		httpClient.execute(HttpRequest.get("http://127.0.0.1:" + PORT), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(final HttpResponse result) {
				resultObserver.onResult(decodeUTF8(result.getBody()));
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

		assertEquals(decodeUTF8(HelloWorldServer.HELLO_WORLD), resultObserver.get());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test(expected = TimeoutException.class)
	public void testTimeout() throws Throwable {
		final int TIMEOUT = 100;
		final Eventloop eventloop = new Eventloop();

		final AsyncHttpServer httpServer = new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				eventloop.schedule(eventloop.currentTimeMillis() + (3 * TIMEOUT), new Runnable() {
					@Override
					public void run() {
						callback.onResult(HttpResponse.create().body(TIMEOUT_EXCEPTION_BYTES));
					}
				});
			}
		});
		httpServer.setListenPort(PORT);

		final AsyncHttpClient httpClient = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, 3_000L, Utils.forString("8.8.8.8")));
		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		httpServer.listen();

		httpClient.execute(HttpRequest.get("http://127.0.0.1:" + PORT), TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(decodeUTF8(result.getBody()));
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

		try {
			System.err.println("Result: " + resultObserver.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = TimeoutException.class)
	public void testClientTimeoutConnect() throws Throwable {
		final int TIMEOUT = 1;
		final Eventloop eventloop = new Eventloop();

		final AsyncHttpClient httpClient = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, 3_000L, Utils.forString("8.8.8.8")));
		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		httpClient.execute(HttpRequest.get("http://google.com"), TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(decodeUTF8(result.getBody()));
				httpClient.close();
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.onException(exception);
				httpClient.close();
			}
		});

		eventloop.run();

		try {
			System.err.println("Result: " + resultObserver.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = RuntimeException.class)
	public void testBigHttpMessage() throws Throwable {
		final int TIMEOUT = 1000;
		final Eventloop eventloop = new Eventloop();

		final AsyncHttpServer httpServer = HelloWorldServer.helloWorldServer(eventloop, PORT);
		final AsyncHttpClient httpClient = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, 3_000L, Utils.forString("8.8.8.8")));
		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		httpServer.listen();

		httpClient.setMaxHttpMessageSize(12);
		httpClient.execute(HttpRequest.get("http://127.0.0.1:" + PORT), TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(decodeUTF8(result.getBody()));
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

		try {
			System.err.println("Result: " + resultObserver.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

}
