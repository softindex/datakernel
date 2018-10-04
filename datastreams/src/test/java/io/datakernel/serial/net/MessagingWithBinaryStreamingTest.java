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

package io.datakernel.serial.net;

import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serial.net.MessagingSerializers.ofJson;
import static io.datakernel.serializer.asm.BufferSerializers.LONG_SERIALIZER;
import static io.datakernel.util.gson.GsonAdapters.INTEGER_JSON;
import static io.datakernel.util.gson.GsonAdapters.STRING_JSON;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class MessagingWithBinaryStreamingTest {
	private static final int LISTEN_PORT = 4821;
	private static final InetSocketAddress address;

	static {
		try {
			address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), LISTEN_PORT);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testPing() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		MessagingSerializer<Integer, Integer> serializer =
				ofJson(INTEGER_JSON);

		SimpleServer server = SimpleServer.create(eventloop,
				new Consumer<AsyncTcpSocket>() {
					void pong(Messaging<Integer, Integer> messaging) {
						messaging.receive()
								.whenResult(msg -> {
									if (msg != null) {
										messaging.send(msg);
										pong(messaging);
									} else {
										messaging.close();
									}
								})
								.whenException(e -> messaging.close());
					}

					@Override
					public void accept(AsyncTcpSocket asyncTcpSocket) {
						MessagingWithBinaryStreaming<Integer, Integer> messaging =
								MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);
						pong(messaging);
					}
				})
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address)
				.whenComplete(new BiConsumer<SocketChannel, Throwable>() {
					void ping(int n, Messaging<Integer, Integer> messaging) {
						messaging.send(n);

						messaging.receive()
								.whenResult(msg -> {
									if (msg != null) {
										if (msg > 0) {
											ping(msg - 1, messaging);
										} else {
											messaging.close();
										}
									} else {
										// empty
									}
								})
								.whenException(e -> messaging.close());
					}

					@Override
					public void accept(SocketChannel socketChannel, Throwable throwable) {
						if (throwable == null) {
							AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
							MessagingWithBinaryStreaming<Integer, Integer> messaging =
									MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);
							ping(3, messaging);
						} else {
							fail("Test Exception: " + throwable);
						}
					}
				});

		eventloop.run();
	}

	@Test
	public void testMessagingDownload() throws Exception {
		List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create();
		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.receive()
							.whenResult(msg -> {
								if (msg != null) {
									assertEquals("start", msg);
									StreamSupplier.ofIterable(source)
											.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
													.withInitialBufferSize(MemSize.of(1)))
											.streamTo(messaging.sendBinaryStream());
								}
							});
				})
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.send("start");
					messaging.sendEndOfStream();

					messaging.receiveBinaryStream()
							.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
							.streamTo(consumerToList);
				});

		eventloop.run();
		assertEquals(source, consumerToList.getList());
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create();
		CompletableFuture<List<Long>> future = consumerToList.getResult().toCompletableFuture();

		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.receive()
							.whenResult(msg -> {
								if (msg != null) {
									assertEquals("start", msg);

									messaging.receiveBinaryStream()
											.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
											.streamTo(consumerToList);

									messaging.sendEndOfStream();
								}
							});
				})
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				});

		eventloop.run();

		assertEquals(source, future.get());
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		boolean[] ack = new boolean[]{false};

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create();
		CompletableFuture<List<Long>> future = consumerToList.getResult().toCompletableFuture();

		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.receive()
							.whenResult(msg -> {
								if (msg != null) {
									assertEquals("start", msg);

									messaging.receiveBinaryStream()
											.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
											.streamTo(consumerToList)
											.whenResult($ -> {
												messaging.send("ack");
												messaging.sendEndOfStream();
											});
								}
							});
				})
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		CompletableFuture<String> future2 = eventloop.connect(address)
				.thenCompose(socketChannel -> {
					AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.send("start");

					StreamSupplier.ofIterable(source)
							.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());

					return messaging.receive()
							.whenResult($ -> messaging.close());
				})
				.toCompletableFuture();

		eventloop.run();

		assertEquals(source, future.get());
		assertEquals("ack", future2.get());
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create();
		CompletableFuture<List<Long>> future = consumerToList.getResult().toCompletableFuture();

		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SimpleServer server = SimpleServer.create(eventloop,
				socket -> {
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.receive()
							.whenResult(msg -> {
								if (msg != null) {
									assertEquals("start", msg);

									messaging.sendEndOfStream();

									messaging.receiveBinaryStream()
											.apply(SerialBinaryDeserializer.create(LONG_SERIALIZER))
											.streamTo(consumerToList);
								}
							});
				})
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					MessagingWithBinaryStreaming<String, String> messaging =
							MessagingWithBinaryStreaming.create(socket, serializer);

					messaging.send("start");

					StreamSupplier.ofIterator(LongStream.range(0, 100).boxed().iterator())
							.apply(SerialBinarySerializer.create(LONG_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(messaging.sendBinaryStream());
				});

		eventloop.run();

		assertEquals(LongStream.range(0, 100).boxed().collect(toList()), future.get());
	}

}
