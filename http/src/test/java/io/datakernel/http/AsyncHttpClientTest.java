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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.eventloop.SimpleServer.SocketHandlerProvider;
import io.datakernel.exception.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeUtf8;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AsyncHttpClientTest {
	private static final int PORT = 45788;
	public static final byte[] TIMEOUT_EXCEPTION_BYTES = encodeAscii("ERROR: Must be TimeoutException");

	public static final int TIMEOUT = 1000;

	private static final InetAddress GOOGLE_PUBLIC_DNS = HttpUtils.inetAddress("8.8.8.8");

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testAsyncClient() throws Exception {
		final Eventloop eventloop = Eventloop.create();

		final AsyncHttpServer httpServer = HelloWorldServer.helloWorldServer(eventloop, PORT);
		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop).withDnsServerAddress(GOOGLE_PUBLIC_DNS));
		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		httpServer.listen();

		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(final HttpResponse result) {
				try {
					resultObserver.onResult(decodeUtf8(result.getBody()));
				} catch (ParseException e) {
					onException(e);
				}
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

		assertEquals(decodeUtf8(HelloWorldServer.HELLO_WORLD), resultObserver.get());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test(expected = TimeoutException.class)
	public void testTimeout() throws Throwable {
		final int TIMEOUT = 100;
		final Eventloop eventloop = Eventloop.create();

		final AsyncHttpServer httpServer = AsyncHttpServer.create(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final Callback callback) {
				eventloop.schedule(eventloop.currentTimeMillis() + (3 * TIMEOUT), new Runnable() {
					@Override
					public void run() {
						callback.onResult(HttpResponse.ok200().withBody(TIMEOUT_EXCEPTION_BYTES));
					}
				});
			}
		}).withListenPort(PORT);

		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop).withDnsServerAddress(GOOGLE_PUBLIC_DNS));
		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		httpServer.listen();

		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				try {
					resultObserver.onResult(decodeUtf8(result.getBody()));
				} catch (ParseException e) {
					onException(e);
				}
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
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		try {
			System.err.println("Result: " + resultObserver.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test(expected = TimeoutException.class)
	public void testClientTimeoutConnect() throws Throwable {
		final int TIMEOUT = 1;
		final Eventloop eventloop = Eventloop.create();

		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop).withDnsServerAddress(GOOGLE_PUBLIC_DNS));
		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		httpClient.send(HttpRequest.get("http://google.com"), TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				try {
					resultObserver.onResult(decodeUtf8(result.getBody()));
				} catch (ParseException e) {
					onException(e);
				}
				httpClient.close();
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.onException(exception);
				httpClient.close();
			}
		});

		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		try {
			System.err.println("Result: " + resultObserver.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test(expected = ParseException.class)
	public void testBigHttpMessage() throws Throwable {
		final int TIMEOUT = 1000;
		final Eventloop eventloop = Eventloop.create();

		final AsyncHttpServer httpServer = HelloWorldServer.helloWorldServer(eventloop, PORT);
		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop).withDnsServerAddress(GOOGLE_PUBLIC_DNS));
		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		httpServer.listen();

		httpClient.withMaxHttpMessageSize(12);
		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				try {
					resultObserver.onResult(decodeUtf8(result.getBody()));
				} catch (ParseException e) {
					onException(e);
				}
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
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		try {
			System.err.println("Result: " + resultObserver.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test(expected = ParseException.class)
	public void testEmptyLineResponse() throws Throwable {
		final Eventloop eventloop = Eventloop.create();

		final SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {
			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(final AsyncTcpSocket asyncTcpSocket) {
				return new AsyncTcpSocket.EventHandler() {
					@Override
					public void onRegistered() {
						asyncTcpSocket.read();
					}

					@Override
					public void onRead(ByteBuf buf) {
						buf.recycle();
						asyncTcpSocket.write(ByteBufStrings.wrapAscii("\r\n"));
					}

					@Override
					public void onReadEndOfStream() {
						// empty
					}

					@Override
					public void onWrite() {
						asyncTcpSocket.close();
					}

					@Override
					public void onClosedWithError(Exception e) {
						// empty
					}
				};
			}
		};

		final SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider).withListenPort(PORT);
		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop).withDnsServerAddress(GOOGLE_PUBLIC_DNS));
		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		server.listen();

		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), TIMEOUT, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				try {
					resultObserver.onResult(decodeUtf8(result.getBody()));
				} catch (ParseException e) {
					onException(e);
				}
				httpClient.close();
			}

			@Override
			public void onException(Exception e) {
				resultObserver.onException(e);
				httpClient.close();
				server.close();
			}
		});

		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		try {
			System.err.println("Result: " + resultObserver.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
		assertThat(eventloop, doesntHaveFatals());
	}
}
