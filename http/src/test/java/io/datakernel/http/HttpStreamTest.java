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

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

public class HttpStreamTest {
	public static final int PORT = 33453;
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();
	public Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	public AsyncHttpServer server;
	public AsyncHttpClient client;
	public String requestBody = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor.\n" +
			"Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.\n" +
			"Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim.\n" +
			"Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu.\n" +
			"In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium.";

	public Random random = new Random();
	public List<ByteBuf> expectedList;

	@Before
	public void setUp() {
		client = AsyncHttpClient.create(eventloop);
		expectedList = getBufsList(requestBody.getBytes());
	}

	@Test
	public void testStreamUpload() throws IOException {
		server = AsyncHttpServer.create(eventloop,
				request -> request.getBodyStream().async().toList()
						.thenApply(list -> list.stream()
								.map(buf -> buf.asString(UTF_8))
								.collect(Collectors.joining("")))
						.thenCompose(s -> {
							assertEquals(requestBody, s);
							return Stage.of(HttpResponse.ok200());
						}))
				.withListenPort(PORT);
		server.listen();

		client = AsyncHttpClient.create(eventloop);
		client.requestBodyStream(HttpRequest.post("http://127.0.0.1:" + PORT)
				.withBodyStream(SerialSupplier.ofIterable(expectedList)
						.transformAsync(item -> getCurrentEventloop().delay(1, item))))
				.async()
				.whenComplete(assertComplete(response -> {
					assertEquals(200, response.getCode());
					close();
				}));

		eventloop.run();
	}

	@Test
	public void testStreamDownload() throws IOException {
		server = AsyncHttpServer.create(eventloop,
				request -> Stage.of(
						HttpResponse.ok200()
								.withBodyStream(SerialSupplier.ofIterable(expectedList)
										.transformAsync(item -> getCurrentEventloop().delay(1, item)))))
				.withListenPort(PORT);
		server.listen();

		client.requestBodyStream(HttpRequest.post("http://127.0.0.1:" + PORT))
				.async()
				.thenApply(response -> {
					assertEquals(200, response.getCode());
					return response.getBodyStream().async();
				})
				.thenCompose(SerialSupplier::toList)
				.thenApply(list -> list.stream()
						.map(buf -> buf.asString(UTF_8))
						.collect(Collectors.joining("")))
				.whenComplete(assertComplete(string ->
				{
					assertEquals(requestBody, string);
					close();
				}));

		eventloop.run();
	}

	@Test
	public void testLoopBack() throws IOException {

		server = AsyncHttpServer.create(eventloop,
				request ->
						request.getBodyStream().async().toList()
								.thenApply(SerialSupplier::ofIterable)
								.thenCompose(bodyStream -> Stage.of(
										HttpResponse.ok200()
												.withBodyStream(bodyStream.async()))))
				.withListenPort(PORT);
		server.listen();

		client.requestBodyStream(
				HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(SerialSupplier.ofIterable(expectedList)
								.transformAsync(item -> getCurrentEventloop().delay(1, item))))
				.thenApply(response -> {
					assertEquals(200, response.getCode());
					return response.getBodyStream().async();
				})
				.thenCompose(SerialSupplier::toList)
				.thenApply(list -> list.stream()
						.map(buf -> buf.asString(UTF_8))
						.collect(joining("")))
				.whenComplete(assertComplete(string -> {
					assertEquals(requestBody, string);
					close();
				}));

		eventloop.run();
	}

	@Test
	public void testCloseWithError() throws IOException {
		String exceptionMessage = "Test Exception";

		server = AsyncHttpServer.create(eventloop,
				request -> {
					SerialSupplier<ByteBuf> stream = request.getBodyStream();
					return stream.get()
							.thenCompose(buf -> {
								HttpException testException = new HttpException(432, exceptionMessage);
								stream.closeWithError(testException);
								buf.recycle();
								return Stage.ofException(testException);
							});
				})
				.withListenPort(PORT);
		server.listen();

		SerialSupplier<ByteBuf> supplier = SerialSupplier.ofIterable(expectedList);
		// SerialSupplier<ByteBuf> supplier = new DelayingSupplier<>(5, expectedList);

		client.requestBodyStream(
				HttpRequest.post("http://127.0.0.1:" + PORT)
						.withBodyStream(supplier))
				.thenApply(HttpMessage::getBodyStream)
				.thenCompose(SerialSupplier::toList)
				.whenComplete(assertComplete(list -> {
							assertEquals(1, list.size());
							assertEquals(exceptionMessage, list.get(0).getString(UTF_8));
							supplier.close();
							close();
						}
				));

		eventloop.run();
	}

	private void close() {
		server.close();
		client.stop();
	}

	private List<ByteBuf> getBufsList(byte[] array) {
		List<ByteBuf> list = new ArrayList<>();
		ByteBuf buf = ByteBufPool.allocate(array.length);
		buf.put(array);
		int bufSize = random.nextInt(array.length) + 5;
		for (int i = 0; i < array.length; i += bufSize) {
			int min = min(bufSize, buf.readRemaining());
			list.add(buf.slice(min));
			buf.moveReadPosition(min);
		}
		buf.recycle();
		return list;
	}

}
