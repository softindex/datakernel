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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AsyncTcpSocketNio;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.common.Recyclable.deepRecycle;
import static io.datakernel.http.stream.BufsConsumerChunkedDecoder.CRLF;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public final class HttpStreamTest {
	private static final int PORT = getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private String requestBody = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor.\n" +
			"Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.\n" +
			"Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim.\n" +
			"Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu.\n" +
			"In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium.";

	private List<ByteBuf> expectedList;

	@Before
	public void setUp() {
		expectedList = getBufsList(requestBody.getBytes());
	}

	@Test
	public void testStreamUpload() throws IOException {
		startTestServer(request -> request
				.getBodyStream()
				.async()
				.toCollector(ByteBufQueue.collector())
				.whenComplete(assertComplete(buf -> assertEquals(requestBody, buf.asString(UTF_8))))
				.then(s -> Promise.of(HttpResponse.ok200())));

		Integer code = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(ChannelSupplier.ofIterable(expectedList)
								.mapAsync(item -> Promises.delay(1L, item))))
				.async()
				.map(HttpResponse::getCode));

		assertEquals((Integer) 200, code);
	}

	@Test
	public void testStreamDownload() throws IOException {
		startTestServer(request ->
				HttpResponse.ok200()
						.withBodyStream(ChannelSupplier.ofIterable(expectedList)
								.mapAsync(item -> Promises.delay(1L, item))));

		ByteBuf body = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT))
				.async()
				.whenComplete(assertComplete(response -> assertEquals(200, response.getCode())))
				.then(response -> response.getBodyStream().async().toCollector(ByteBufQueue.collector())));

		assertEquals(requestBody, body.asString(UTF_8));
	}

	@Test
	public void testLoopBack() throws IOException {
		startTestServer(request -> request
				.getBodyStream()
				.async()
				.toList()
				.map(ChannelSupplier::ofIterable)
				.then(bodyStream -> Promise.of(HttpResponse.ok200().withBodyStream(bodyStream.async()))));

		ByteBuf body = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(ChannelSupplier.ofIterable(expectedList)
								.mapAsync(item -> Promises.delay(1L, item))))
				.whenComplete(assertComplete(response -> assertEquals(200, response.getCode())))
				.then(response -> response.getBodyStream().async().toCollector(ByteBufQueue.collector())));

		assertEquals(requestBody, body.asString(UTF_8));
	}

	@Test
	public void testCloseWithError() throws IOException {
		String exceptionMessage = "Test Exception";

		startTestServer(request -> Promise.ofException(new HttpException(432, exceptionMessage)));

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofIterable(expectedList);

		ByteBuf body = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(supplier))
				.then(response -> response.getBodyStream().toCollector(ByteBufQueue.collector())));

		assertTrue(body.asString(UTF_8).contains(exceptionMessage));
	}

	@Test
	public void testChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"4" + crlf + "Test" + crlf + "0" + crlf + crlf;

		String responseMessage =
				"HTTP/1.1 200 OK" + crlf +
						"Content-Length: 4" + crlf +
						"Connection: keep-alive" + crlf + crlf +
						"Test";

		ByteBuf body = await(AsyncTcpSocketNio.connect(new InetSocketAddress(PORT))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then($ -> socket.write(null))
						.then($ -> ChannelSupplier.ofSocket(socket).toCollector(ByteBufQueue.collector()))
						.whenComplete(socket::close)));

		assertEquals(responseMessage, body.asString(UTF_8));

		deepRecycle(expectedList); // not used here
	}

	@Test
	public void testMalformedChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"ffffffffff";

		ByteBuf body = await(AsyncTcpSocketNio.connect(new InetSocketAddress(PORT))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then($ -> socket.read())
						.whenComplete(socket::close)));

		assertNull(body);

//		String response = body.asString(UTF_8);
//		System.out.println(response);
//		assertTrue(response.contains("400"));
//		assertTrue(response.contains("Malformed chunk length"));

		deepRecycle(expectedList); // not used here
	}

	@Test
	public void testTruncatedRequest() throws IOException {
		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Content-Length: 13" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"3";

		ByteBuf body = await(AsyncTcpSocketNio.connect(new InetSocketAddress(PORT))
				.then(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.then($ -> socket.write(null))
						.then($ -> socket.read())
						.whenComplete(socket::close)));

		assertNull(body);

//		String response = body.asString(UTF_8);
//		assertTrue(response.contains("HTTP/1.1 400 Bad Request"));
//		assertTrue(response.contains("Incomplete HTTP message"));

		deepRecycle(expectedList); // not used here
	}

	@Test
	public void testSendingErrors() throws IOException {
		Exception exception = new Exception("Test Exception");

		startTestServer(request -> request.loadBody().map(body -> HttpResponse.ok200().withBody(body.slice())));

		Throwable e = awaitException(
				AsyncHttpClient.create(Eventloop.getCurrentEventloop())
						.request(HttpRequest.post("http://127.0.0.1:" + PORT)
								.withBodyStream(ChannelSuppliers.concat(
										ChannelSupplier.ofIterable(expectedList),
										ChannelSupplier.ofException(exception))))
						.then(response -> response.getBodyStream().toCollector(ByteBufQueue.collector())));

		assertSame(e, exception);
	}

	private void startTestServer(AsyncServlet servlet) throws IOException {
		AsyncHttpServer.create(Eventloop.getCurrentEventloop(), servlet)
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();
	}

	private List<ByteBuf> getBufsList(byte[] array) {
		List<ByteBuf> list = new ArrayList<>();
		ByteBuf buf = ByteBufPool.allocate(array.length);
		buf.put(array);
		int bufSize = ThreadLocalRandom.current().nextInt(array.length) + 5;
		for (int i = 0; i < array.length; i += bufSize) {
			int min = min(bufSize, buf.readRemaining());
			list.add(buf.slice(min));
			buf.moveHead(min);
		}
		buf.recycle();
		return list;
	}
}
