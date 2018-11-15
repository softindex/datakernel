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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.InvalidSizeException;
import io.datakernel.exception.UnknownFormatException;
import io.datakernel.http.AsyncHttpClient.JmxInspector;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class AsyncHttpClientTest {
	private static final int PORT = 45788;

	private static final byte[] HELLO_WORLD = encodeAscii("Hello, World!");

	public static void startServer() throws IOException {
		AsyncHttpServer.create(Eventloop.getCurrentEventloop(), request ->
				Promise.of(HttpResponse.ok200()
						.withBodyStream(SerialSupplier.ofStream(
								IntStream.range(0, HELLO_WORLD.length)
										.mapToObj(idx -> {
											ByteBuf buf = ByteBufPool.allocate(1);
											buf.put(HELLO_WORLD[idx]);
											return buf;
										})))))
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();
	}

	@Test
	public void testAsyncClient() throws Exception {
		startServer();

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.thenCompose(ensureResponseBody())
				.thenApply(HttpMessage::getBody)
				.whenComplete(assertComplete(buf -> assertEquals(decodeAscii(HELLO_WORLD), buf.asString(UTF_8))));
	}

	@Test
	public void testClientTimeoutConnect() {
		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withConnectTimeout(Duration.ofMillis(1))
				.request(HttpRequest.get("http://google.com"))
				.thenCompose(ensureResponseBody())
				.whenComplete(assertFailure(AsyncTimeoutException.class, "Connection timed out"));
	}

	@Test
	public void testBigHttpMessage() throws IOException {
		startServer();

		int maxBodySize = HELLO_WORLD.length - 1;

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.thenCompose(ensureResponseBody(maxBodySize))
				.whenComplete(assertFailure(InvalidSizeException.class, "ByteBufQueue exceeds maximum size of " + maxBodySize + " bytes"));
	}

	@Test
	public void testEmptyLineResponse() throws IOException {
		SimpleServer.create(socket ->
				socket.read()
						.whenResult(ByteBuf::recycle)
						.thenCompose($ -> socket.write(wrapAscii("\r\n")))
						.whenComplete(($, e) -> socket.close()))
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.get("http://127.0.0.1:" + PORT))
				.thenCompose(ensureResponseBody())
				.whenComplete(assertFailure(UnknownFormatException.class, "Invalid response"));
	}

	@Test
	public void testActiveRequestsCounter() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<SettablePromise<HttpResponse>> responses = new ArrayList<>();

		AsyncHttpServer server = AsyncHttpServer.create(eventloop,
				request -> Promise.ofCallback(responses::add))
				.withListenPort(PORT);

		server.listen();

		JmxInspector inspector = new JmxInspector();
		AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop)
				.withNoKeepAlive()
				.withConnectTimeout(Duration.ofMillis(20))
				.withReadWriteTimeout(Duration.ofMillis(20))
				.withInspector(inspector);

		Promises.all(
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT))
						.thenCompose(ensureResponseBody()),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT))
						.thenCompose(ensureResponseBody()),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT))
						.thenCompose(ensureResponseBody()),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT))
						.thenCompose(ensureResponseBody()),
				httpClient.request(HttpRequest.get("http://127.0.0.1:" + PORT))
						.thenCompose(ensureResponseBody()))
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
	}
}
