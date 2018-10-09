/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ActiveStagesRule;
import io.datakernel.stream.processor.ByteBufRule;
import org.hamcrest.beans.HasPropertyWithValue;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsSame;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HelloWorldServer.HELLO_WORLD;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class AsyncHttpClientTest {
	private static final int PORT = 45788;

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Rule
	public ActiveStagesRule activeStagesRule = new ActiveStagesRule();

	public static AsyncHttpServer helloWorldServer(Eventloop primaryEventloop, int port) {
		return AsyncHttpServer.create(primaryEventloop,
				request ->
						Stage.of(HttpResponse.ok200()
								.withBodyStream(SerialSupplier.ofStream(
										IntStream.range(0, HELLO_WORLD.length)
												.mapToObj(idx -> {
													ByteBuf buf = ByteBufPool.allocate(1);
													buf.put(HELLO_WORLD[idx]);
													return buf;
												})))))
				.withListenAddress(new InetSocketAddress("localhost", port));
	}

	@Test
	public void testAsyncClient() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer httpServer = helloWorldServer(eventloop, PORT);
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

		httpServer.listen();

		CompletableFuture<String> future = httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://127.0.0.1:" + PORT))
				.thenApply(HttpMessage::getBody)
				.thenApply(buf -> buf.asString(UTF_8))
				.whenComplete(($, e) -> {
					httpClient.stop();
					httpServer.close();
				})
				.toCompletableFuture();

		eventloop.run();

		assertEquals(decodeAscii(HELLO_WORLD), future.get());
	}

	@Test
	public void testClientTimeoutConnect() throws ExecutionException, InterruptedException {
		Duration TIMEOUT = Duration.ofMillis(1);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop).withConnectTimeout(TIMEOUT);

		CompletableFuture<String> future = httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://google.com"))
				.thenApply(response -> response.getBody().asString(UTF_8))
				.whenComplete(($, e) -> httpClient.stop())
				.toCompletableFuture();

		eventloop.run();

		expectedException.expect(ExecutionException.class);
		expectedException.expectCause(IsSame.sameInstance(Eventloop.CONNECT_TIMEOUT));
			System.err.println("Result: " + future.get());
	}

	@Test
	public void testBigHttpMessage() throws IOException, ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		AsyncHttpServer httpServer = helloWorldServer(eventloop, PORT);
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

		httpServer.listen();

		int maxBodySize = HELLO_WORLD.length - 1;
		CompletableFuture<String> future = httpClient.requestWithResponseBody(maxBodySize, HttpRequest.get("http://127.0.0.1:" + PORT))
				.thenApply(response -> response.getBody().asString(UTF_8))
				.whenComplete(($, e) -> {
					httpClient.stop();
					httpServer.close();
				})
				.toCompletableFuture();

		eventloop.run();

		expectedException.expect(ExecutionException.class);
		expectedException.expectCause(HasPropertyWithValue.hasProperty("message", IsEqual.equalTo("ByteBufQueue exceeds maximum size of " + maxBodySize + " bytes")));
		System.err.println("Result: " + future.get());
	}

	@Test
	public void testEmptyLineResponse() throws IOException, ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> socket.read()
						.whenResult(ByteBuf::recycle)
						.thenCompose($ -> socket.write(wrapAscii("\r\n")))
						.whenComplete(($, e) -> socket.close()))
				.withListenAddress(new InetSocketAddress("localhost", PORT));
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

		server.listen();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT);
		CompletableFuture<String> future = httpClient.requestWithResponseBody(Integer.MAX_VALUE, request)
				.thenApply(response -> response.getBody().asString(UTF_8))
				.whenComplete(($, e) -> {
					httpClient.stop();
					server.close();
				})
				.toCompletableFuture();

		eventloop.run();

		expectedException.expect(ExecutionException.class);
		expectedException.expectCause(IsSame.sameInstance(HttpClientConnection.INVALID_RESPONSE));
		System.err.println("Result: " + future.get());
	}

	@Test
	public void testActiveRequestsCounter() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		List<SettableStage<HttpResponse>> responses = new ArrayList<>();
		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> Stage.ofCallback(responses::add))
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		server.listen();

		AsyncHttpClient.JmxInspector inspector = new AsyncHttpClient.JmxInspector();
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop)
				.withNoKeepAlive()
				.withConnectTimeout(Duration.ofMillis(20))
				.withReadWriteTimeout(Duration.ofMillis(20))
				.withInspector(inspector);

		Stages.all(
				httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://127.0.0.1:" + PORT)),
				httpClient.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get("http://127.0.0.1:" + PORT)))
				.whenComplete(($, e) -> {
					server.close();
					responses.forEach(response -> response.set(HttpResponse.ok200()));

					inspector.getTotalRequests().refresh(eventloop.currentTimeMillis());
					inspector.getHttpTimeouts().refresh(eventloop.currentTimeMillis());

					System.out.println(inspector.getTotalRequests().getTotalCount());
					System.out.println();
					System.out.println(inspector.getHttpTimeouts().getTotalCount());
					System.out.println(inspector.getResolveErrors().getTotal());
					System.out.println(inspector.getConnectErrors().getTotal());
					System.out.println(inspector.getTotalResponses());

					assertEquals(4, inspector.getActiveRequests());
				})
				.whenComplete(assertFailure(AsyncTimeoutException.class, "timeout"));

		eventloop.run();
	}
}
