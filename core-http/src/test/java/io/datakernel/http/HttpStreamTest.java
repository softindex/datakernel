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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.async.Promise.ofCallback;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.http.stream.BufsConsumerChunkedDecoder.CRLF;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static io.datakernel.util.Recyclable.deepRecycle;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
public final class HttpStreamTest {
	private static final int PORT = 33453;

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
				.thenCompose(s -> Promise.of(HttpResponse.ok200())));

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(ChannelSupplier.ofIterable(expectedList)
								.mapAsync(item -> ofCallback(cb ->
										getCurrentEventloop().delay(1, () -> cb.set(item))))))
				.async()
				.whenComplete(assertComplete(response -> assertEquals(200, response.getCode())));
	}

	@Test
	public void testStreamDownload() throws IOException {
		startTestServer(request -> Promise.of(
				HttpResponse.ok200()
						.withBodyStream(ChannelSupplier.ofIterable(expectedList)
								.mapAsync(item -> ofCallback(cb ->
										getCurrentEventloop().delay(1, () -> cb.set(item)))))));

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT))
				.async()
				.whenComplete(assertComplete(response -> assertEquals(200, response.getCode())))
				.thenCompose(response -> response.getBodyStream().async().toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf -> assertEquals(requestBody, buf.asString(UTF_8))));
	}

	@Test
	public void testLoopBack() throws IOException {
		startTestServer(request -> request
				.getBodyStream()
				.async()
				.toList()
				.thenApply(ChannelSupplier::ofIterable)
				.thenCompose(bodyStream -> Promise.of(HttpResponse.ok200().withBodyStream(bodyStream.async()))));

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(ChannelSupplier.ofIterable(expectedList)
								.mapAsync(item -> ofCallback(cb ->
										getCurrentEventloop().delay(1, () -> cb.set(item))))))
				.whenComplete(assertComplete(response -> assertEquals(200, response.getCode())))
				.thenCompose(response -> response.getBodyStream().async().toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf -> assertEquals(requestBody, buf.asString(UTF_8))));
	}

	@Test
	public void testCloseWithError() throws IOException {
		String exceptionMessage = "Test Exception";

		startTestServer(request -> Promise.ofException(new HttpException(432, exceptionMessage)));

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofIterable(expectedList);

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(supplier))
				.thenCompose(response -> response.getBodyStream().toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf -> assertEquals(exceptionMessage, buf.asString(UTF_8))));
	}

	@Test
	public void testChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.getBody().thenApply(body -> HttpResponse.ok200().withBody(body)));

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

		AsyncTcpSocketImpl.connect(new InetSocketAddress(PORT))
				.thenCompose(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.thenCompose($ -> socket.read())
						.whenComplete(assertComplete(responseBuf -> assertEquals(responseMessage, responseBuf.asString(UTF_8))))
						.whenComplete(($, e) -> socket.close()));

		deepRecycle(expectedList); // not used here
	}

	@Test
	public void testMalformedChunkedEncodingMessage() throws IOException {
		startTestServer(request -> request.getBody().thenApply(body -> HttpResponse.ok200().withBody(body)));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"ffffffffff";

		AsyncTcpSocketImpl.connect(new InetSocketAddress(PORT))
				.thenCompose(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.thenCompose($ -> socket.read())
						.whenComplete(assertComplete(responseBuf -> {
							String response = responseBuf.asString(UTF_8);
							System.out.println(response);
							assertTrue(response.contains("400"));
							assertTrue(response.contains("Malformed chunk length"));
						}))
						.whenComplete(($, e) -> socket.close()));

		deepRecycle(expectedList); // not used here
	}

	@Test
	public void testTruncatedRequest() throws IOException {
		startTestServer(request -> request.getBody().thenApply(body -> HttpResponse.ok200().withBody(body)));

		String crlf = new String(CRLF, UTF_8);

		String chunkedRequest =
				"POST / HTTP/1.1" + crlf +
						"Host: localhost" + crlf +
						"Content-Length: 13" + crlf +
						"Transfer-Encoding: chunked" + crlf + crlf +
						"3";

		AsyncTcpSocketImpl.connect(new InetSocketAddress(PORT))
				.thenCompose(socket -> socket.write(ByteBuf.wrapForReading(chunkedRequest.getBytes(UTF_8)))
						.thenCompose($ -> socket.write(null))
						.thenCompose($ -> socket.read())
						.whenComplete(assertComplete(responseBuf -> {
							String response = responseBuf.asString(UTF_8);
							assertTrue(response.contains("HTTP/1.1 400 Bad Request"));
							assertTrue(response.contains("Incomplete HTTP message"));
						}))

						.whenComplete(($, e) -> socket.close()));

		deepRecycle(expectedList); // not used here
	}

	@Test
	public void testSendingErrors() throws IOException {
		String exceptionMessage = "Test Exception";

		startTestServer(request -> request.getBody().thenApply(body -> HttpResponse.ok200().withBody(body)));

		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(
				ChannelSupplier.ofIterable(expectedList),
				ChannelSupplier.ofException(new Exception(exceptionMessage))
		);

		AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.request(HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(supplier))
				.thenCompose(response -> response.getBodyStream().toCollector(ByteBufQueue.collector()))
				.whenComplete(assertFailure(exceptionMessage));
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
			buf.moveReadPosition(min);
		}
		buf.recycle();
		return list;
	}
}
