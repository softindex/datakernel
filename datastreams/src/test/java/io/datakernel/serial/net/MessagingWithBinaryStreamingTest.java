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

package io.datakernel.serial.net;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.MemSize;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.LongStream;

import static io.datakernel.json.GsonAdapters.INTEGER_JSON;
import static io.datakernel.json.GsonAdapters.STRING_JSON;
import static io.datakernel.serial.net.ByteBufSerializers.ofJson;
import static io.datakernel.serializer.asm.BufferSerializers.LONG_SERIALIZER;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class MessagingWithBinaryStreamingTest {
	private static final int LISTEN_PORT = 4821;
	public static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", LISTEN_PORT);

	private static ByteBufSerializer<Integer, Integer> INTEGER_SERIALIZER = ofJson(INTEGER_JSON);
	private static ByteBufSerializer<String, String> STRING_SERIALIZER = ofJson(STRING_JSON);

	private static void pong(Messaging<Integer, Integer> messaging) {
		messaging.receive()
				.thenCompose(msg -> {
					if (msg != null) {
						return messaging.send(msg).whenResult($ -> pong(messaging));
					}
					messaging.close();
					return Promise.complete();
				})
				.whenException(e -> messaging.close());
	}

	private static void ping(int n, Messaging<Integer, Integer> messaging) {
		messaging.send(n)
				.thenCompose($ -> messaging.receive())
				.whenResult(msg -> {
					if (msg != null) {
						if (msg > 0) {
							ping(msg - 1, messaging);
						} else {
							messaging.close();
						}
					}
				})
				.whenException(e -> messaging.close());
	}

	@Test
	public void testPing() throws Exception {
		SimpleServer.create(socket ->
				pong(MessagingWithBinaryStreaming.create(socket, INTEGER_SERIALIZER)))
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.whenComplete(assertComplete(socket -> ping(3, MessagingWithBinaryStreaming.create(socket, INTEGER_SERIALIZER))));
	}

	@Test
	public void testMessagingDownload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.create(socket -> {
			MessagingWithBinaryStreaming<String, String> messaging =
					MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

			messaging.receive()
					.thenCompose(msg -> {
						assertEquals("start", msg);
						return StreamSupplier.ofIterable(source)
								.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
										.withInitialBufferSize(MemSize.of(1)))
								.streamTo(messaging.sendBinaryStream());
					});
		})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenCompose(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

					return messaging.send("start")
							.thenCompose($ -> messaging.sendEndOfStream())
							.thenCompose($ -> messaging.receiveBinaryStream()
									.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
									.toList());
				})
				.whenComplete(assertComplete(list -> assertEquals(source, list)));
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SimpleServer.create(socket -> {
			MessagingWithBinaryStreaming<String, String> messaging =
					MessagingWithBinaryStreaming.create(socket, serializer);

			messaging.receive()
					.whenResult(msg -> assertEquals("start", msg))
					.thenCompose($ ->
							messaging.receiveBinaryStream()
									.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
									.toList()
									.thenCompose(list ->
											messaging.sendEndOfStream().thenApply($2 -> list)))
					.whenComplete(assertComplete(list -> assertEquals(source, list)));
		})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.whenResult(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				});
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SimpleServer.create(socket -> {
			MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(socket, serializer);

			messaging.receive()
					.whenResult(msg -> assertEquals("start", msg))
					.thenCompose(msg ->
							messaging.receiveBinaryStream()
									.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
									.toList()
									.thenCompose(list ->
											messaging.send("ack")
													.thenCompose($ -> messaging.sendEndOfStream())
													.thenApply($ -> list)))
					.whenComplete(assertComplete(list -> assertEquals(source, list)));
		})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenCompose(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					return messaging.send("start")
							.thenCompose($ -> StreamSupplier.ofIterable(source)
									.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
											.withInitialBufferSize(MemSize.of(1)))
									.streamTo(messaging.sendBinaryStream()))
							.thenCompose($ -> messaging.receive())
							.whenComplete(($, e) -> messaging.close());
				})
				.whenComplete(assertComplete(res -> assertEquals("ack", res)));
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.create(socket -> {
			MessagingWithBinaryStreaming<String, String> messaging =
					MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

			messaging.receive()
					.whenResult(msg -> assertEquals("start", msg))
					.thenCompose(msg -> messaging.sendEndOfStream())
					.thenCompose(msg ->
							messaging.receiveBinaryStream()
									.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
									.toList())
					.whenComplete(assertComplete(list -> assertEquals(source, list)));
		})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		AsyncTcpSocketImpl.connect(ADDRESS)
				.whenResult(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				});
	}
}
