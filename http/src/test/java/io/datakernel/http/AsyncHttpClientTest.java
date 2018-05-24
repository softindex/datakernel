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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.eventloop.SimpleServer.SocketHandlerProvider;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.bytebuf.ByteBufStrings.decodeUtf8;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertFailure;
import static org.junit.Assert.assertEquals;

public class AsyncHttpClientTest {
	private static final int PORT = 45788;
	public static final byte[] TIMEOUT_EXCEPTION_BYTES = encodeAscii("ERROR: Must be TimeoutException");

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testAsyncClient() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer httpServer = HelloWorldServer.helloWorldServer(eventloop, PORT);
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

		httpServer.listen();

		CompletableFuture<String> future = httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT)).thenCompose(response -> {
			try {
				return Stage.of(decodeUtf8(response.getBody()));
			} catch (ParseException e) {
				return Stage.ofException(e);
			} finally {
				httpClient.stop();
				httpServer.close();
			}
		}).toCompletableFuture();

		eventloop.run();

		assertEquals(decodeUtf8(HelloWorldServer.HELLO_WORLD), future.get());
	}

	@Test(expected = AsyncTimeoutException.class)
	public void testClientTimeoutConnect() throws Throwable {
		Duration TIMEOUT = Duration.ofMillis(1);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop).withConnectTimeout(TIMEOUT);

		CompletableFuture<String> future = httpClient.send(HttpRequest.get("http://google.com"))
				.thenCompose(response -> {
					try {
						return Stage.of(decodeUtf8(response.getBody()));
					} catch (ParseException e) {
						return Stage.ofException(e);
					}
				})
				.whenComplete((s, throwable) -> {
					httpClient.stop();
				})
				.toCompletableFuture();

		eventloop.run();

		try {
			System.err.println("Result: " + future.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = ParseException.class)
	public void testBigHttpMessage() throws Throwable {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer httpServer = HelloWorldServer.helloWorldServer(eventloop, PORT);
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop).withMaxHttpMessageSize(MemSize.of(12));

		httpServer.listen();

		CompletableFuture<String> future = httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT)).thenCompose(response -> {
			try {
				return Stage.of(decodeUtf8(response.getBody()));
			} catch (ParseException e) {
				return Stage.ofException(e);
			}
		}).whenComplete((s, throwable) -> {
			httpClient.stop();
			httpServer.close();
		}).toCompletableFuture();

		eventloop.run();

		try {
			System.err.println("Result: " + future.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = ParseException.class)
	public void testEmptyLineResponse() throws Throwable {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> new AsyncTcpSocket.EventHandler() {
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

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider).withListenAddress(new InetSocketAddress("localhost", PORT));
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

		server.listen();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT);
		CompletableFuture<String> future = httpClient.send(request).thenCompose(response -> {
			try {
				return Stage.of(decodeUtf8(response.getBody()));
			} catch (ParseException e) {
				return Stage.ofException(e);
			}
		}).whenComplete((s, throwable) -> {
			httpClient.stop();
			server.close();
		}).toCompletableFuture();

		eventloop.run();

		try {
			System.err.println("Result: " + future.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test
	public void testActiveRequestsCounter() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncServlet slowloris = AsyncServlet.of(request -> {
			request.recycleBufs();
			return new SettableStage<>();
		});
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, slowloris)
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		server.listen();

		AsyncHttpClient.JmxInspector inspector = new AsyncHttpClient.JmxInspector();
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop)
				.withNoKeepAlive()
				.withConnectTimeout(Duration.ofMillis(20))
				.withReadTimeout(Duration.ofMillis(20))
				.withWriteTimeout(Duration.ofMillis(20))
				.withInspector(inspector);

		Stages.all(
				httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT))
		)
				.whenComplete(($, e) -> {
					server.close();

					inspector.getTotalRequests().refresh(eventloop.currentTimeMillis());
					inspector.getHttpTimeouts().refresh(eventloop.currentTimeMillis());

//					System.out.println(inspector.getTotalRequests().getTotalCount());
//					System.out.println();
//					System.out.println(inspector.getHttpTimeouts().getTotalCount());
//					System.out.println(inspector.getResolveErrors().getTotal());
//					System.out.println(inspector.getConnectErrors().getTotal());
//					System.out.println(inspector.responsesErrors);
//					System.out.println(inspector.responses);

					assertEquals(0, inspector.getActiveRequests());
				})
				.whenComplete(assertFailure(AsyncTimeoutException.class, "timeout"));

		eventloop.run();
	}
}
