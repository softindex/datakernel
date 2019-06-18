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

package io.datakernel.csp.net;

import io.datakernel.async.Promise;
import io.datakernel.csp.binary.ByteBufSerializer;
import io.datakernel.csp.process.ChannelDeserializer;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import io.datakernel.util.MemSize;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.LongStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.serializer.util.BinarySerializers.LONG_SERIALIZER;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public final class MessagingWithBinaryStreamingTest {
	private static final int LISTEN_PORT = getFreePort();
	public static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", LISTEN_PORT);

	private static ByteBufSerializer<Integer, Integer> INTEGER_SERIALIZER = ByteBufSerializer.ofJsonCodec(INT_CODEC, INT_CODEC);
	private static ByteBufSerializer<String, String> STRING_SERIALIZER = ByteBufSerializer.ofJsonCodec(STRING_CODEC, STRING_CODEC);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private static void pong(Messaging<Integer, Integer> messaging) {
		messaging.receive()
				.then(msg -> {
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
				.then($ -> messaging.receive())
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

		await(AsyncTcpSocketImpl.connect(ADDRESS)
				.whenComplete(assertComplete(socket -> ping(3, MessagingWithBinaryStreaming.create(socket, INTEGER_SERIALIZER)))));
	}

	@Test
	public void testMessagingDownload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.create(
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

					messaging.receive()
							.whenResult(msg -> {
								assertEquals("start", msg);
								StreamSupplier.ofIterable(source)
										.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
												.withInitialBufferSize(MemSize.of(1)))
										.streamTo(messaging.sendBinaryStream());
							});
				})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		List<Long> list = await(AsyncTcpSocketImpl.connect(ADDRESS)
				.then(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

					return messaging.send("start")
							.then($ -> messaging.sendEndOfStream())
							.then($ -> messaging.receiveBinaryStream()
									.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
									.toList());
				}));

		assertEquals(source, list);
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufSerializer<String, String> serializer =
				ByteBufSerializer.ofJsonCodec(STRING_CODEC, STRING_CODEC);

		SimpleServer.create(
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.receive()
							.whenComplete(assertComplete(msg -> assertEquals("start", msg)))
							.then($ ->
									messaging.receiveBinaryStream()
											.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
											.toList()
											.then(list ->
													messaging.sendEndOfStream().map($2 -> list)))
							.whenComplete(assertComplete(list -> assertEquals(source, list)));
				})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		await(AsyncTcpSocketImpl.connect(ADDRESS)
				.whenResult(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				}));
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		ByteBufSerializer<String, String> serializer =
				ByteBufSerializer.ofJsonCodec(STRING_CODEC, STRING_CODEC);

		SimpleServer.create(
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.receive()
							.whenResult(msg -> assertEquals("start", msg))
							.then(msg ->
									messaging.receiveBinaryStream()
											.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
											.toList()
											.then(list ->
													messaging.send("ack")
															.then($ -> messaging.sendEndOfStream())
															.map($ -> list)))
							.whenComplete(assertComplete(list -> assertEquals(source, list)));
				})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		String msg = await(AsyncTcpSocketImpl.connect(ADDRESS)
				.then(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					return messaging.send("start")
							.then($ -> StreamSupplier.ofIterable(source)
									.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
											.withInitialBufferSize(MemSize.of(1)))
									.streamTo(messaging.sendBinaryStream()))
							.then($ -> messaging.receive())
							.whenComplete(($, e) -> messaging.close());
				}));

		assertEquals("ack", msg);
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		List<Long> source = LongStream.range(0, 100).boxed().collect(toList());

		SimpleServer.create(
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

					messaging.receive()
							.whenComplete(assertComplete(msg -> assertEquals("start", msg)))
							.then(msg -> messaging.sendEndOfStream())
							.then(msg ->
									messaging.receiveBinaryStream()
											.transformWith(ChannelDeserializer.create(LONG_SERIALIZER))
											.toList())
							.whenComplete(assertComplete(list -> assertEquals(source, list)));
				})
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce()
				.listen();

		await(AsyncTcpSocketImpl.connect(ADDRESS)
				.whenResult(socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, STRING_SERIALIZER);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.transformWith(ChannelSerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				}));
	}
}
