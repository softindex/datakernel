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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.async.Promise.ofCallback;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

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