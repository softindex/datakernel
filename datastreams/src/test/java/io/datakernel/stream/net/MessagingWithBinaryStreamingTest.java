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

package io.datakernel.stream.net;

import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.eventloop.SimpleServer.SocketHandlerProvider;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.asm.BufferSerializers.longSerializer;
import static io.datakernel.stream.net.MessagingSerializers.ofJson;
import static io.datakernel.util.gson.GsonAdapters.INTEGER_JSON;
import static io.datakernel.util.gson.GsonAdapters.STRING_JSON;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

	@Before
	public void setup() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testPing() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		MessagingSerializer<Integer, Integer> serializer =
				ofJson(INTEGER_JSON, INTEGER_JSON);

		SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {

			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				MessagingWithBinaryStreaming<Integer, Integer> messaging =
						MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);
				pong(messaging);
				return messaging;
			}

			void pong(Messaging<Integer, Integer> messaging) {
				messaging.receive()
						.thenAccept(msg -> {
							if (msg != null) {
								messaging.send(msg);
								pong(messaging);
							} else {
								messaging.close();
							}
						})
						.whenException(e -> messaging.close());
			}

		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address).whenComplete(new BiConsumer<SocketChannel, Throwable>() {
			void ping(int n, Messaging<Integer, Integer> messaging) {
				messaging.send(n);

				messaging.receive()
						.thenAccept(msg -> {
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
					asyncTcpSocket.setEventHandler(messaging);
					asyncTcpSocket.register();
				} else {
					fail("Test Exception: " + throwable);
				}
			}
		});

		eventloop.run();

		assertEquals(getPoolItemsString(), getCreatedItems(),

				getPoolItems());
	}

	@Test
	public void testMessagingDownload() throws Exception {
		List<Long> source = new ArrayList<>();
//		for (long i = 0; i < 100; i++) {
//			source.add(i);
//		}

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create();
		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> {
			MessagingWithBinaryStreaming<String, String> messaging =
					MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

			messaging.receive()
					.thenAccept(msg -> {
						if (msg != null) {
							assertEquals("start", msg);
							StreamProducer.ofIterable(source)
									.with(StreamBinarySerializer.create(longSerializer())
											.withDefaultBufferSize(1))
									.streamTo(messaging.sendBinaryStream());
						}
					});

			return messaging;
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				MessagingWithBinaryStreaming<String, String> messaging =
						MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

				messaging.send("start");
				messaging.sendEndOfStream();

				messaging.receiveBinaryStream()
						.with(StreamBinaryDeserializer.create(longSerializer()))
						.streamTo(consumerToList);

				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocket.register();
			} else {
				fail("Test Exception: " + throwable);
			}
		});

		eventloop.run();
		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerWithResult<Long, List<Long>> consumerToList = StreamConsumerToList.create();
		CompletableFuture<List<Long>> future = consumerToList.getResult().toCompletableFuture();

		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> {
			MessagingWithBinaryStreaming<String, String> messaging =
					MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

			messaging.receive()
					.thenAccept(msg -> {
						if (msg != null) {
							assertEquals("start", msg);

							messaging.receiveBinaryStream()
									.with(StreamBinaryDeserializer.create(longSerializer()))
									.streamTo(consumerToList);

							messaging.sendEndOfStream();
						}
					});

			return messaging;
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address)
				.whenComplete((socketChannel, throwable) -> {
					if (throwable == null) {
						AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
						MessagingWithBinaryStreaming<String, String> messaging =
								MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

						messaging.send("start");

						StreamProducer.ofIterable(source)
								.with(StreamBinarySerializer.create(longSerializer())
										.withDefaultBufferSize(1))
								.streamTo(messaging.sendBinaryStream());

						asyncTcpSocket.setEventHandler(messaging);
						asyncTcpSocket.register();
					} else {
						fail("Test Exception: " + throwable);
					}
				});

		eventloop.run();

		assertEquals(source, future.get());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		boolean[] ack = new boolean[]{false};

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerWithResult<Long, List<Long>> consumerToList = StreamConsumerToList.create();
		CompletableFuture<List<Long>> future = consumerToList.getResult().toCompletableFuture();

		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SocketHandlerProvider socketHandlerProvider = (AsyncTcpSocket asyncTcpSocket) -> {
			MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

			messaging.receive()
					.thenAccept(msg -> {
						if (msg != null) {
							assertEquals("start", msg);

							messaging.receiveBinaryStream()
									.with(StreamBinaryDeserializer.create(longSerializer()))
									.streamTo(consumerToList)
									.getProducerResult()
									.thenRun(() -> {
										messaging.send("ack");
										messaging.sendEndOfStream();
									});
						}
					});

			return messaging;
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				MessagingWithBinaryStreaming<String, String> messaging =
						MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

				messaging.send("start");

				StreamProducer.ofIterable(source)
						.with(StreamBinarySerializer.create(longSerializer())
								.withDefaultBufferSize(1))
						.streamTo(messaging.sendBinaryStream());

				messaging.receive()
						.thenAccept(msg -> {
							if (msg != null) {
								assertEquals("ack", msg);
								messaging.close();
								ack[0] = true;
							}
						});

				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocket.register();
			} else {
				fail("Test Exception: " + throwable);
			}
		});

		eventloop.run();

		assertEquals(source, future.get());
		assertTrue(ack[0]);

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerWithResult<Long, List<Long>> consumerToList = StreamConsumerWithResult.toList();
		CompletableFuture<List<Long>> future = consumerToList.getResult().toCompletableFuture();

		MessagingSerializer<String, String> serializer =
				ofJson(STRING_JSON, STRING_JSON);

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> {
			MessagingWithBinaryStreaming<String, String> messaging =
					MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

			messaging.receive()
					.thenAccept(msg -> {
						if (msg != null) {
							assertEquals("start", msg);

							messaging.sendEndOfStream();

							messaging.receiveBinaryStream()
									.with(StreamBinaryDeserializer.create(longSerializer()))
									.streamTo(consumerToList);
						}
					});

			return messaging;
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				MessagingWithBinaryStreaming<String, String> messaging =
						MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);

				messaging.send("start");

				StreamProducer.ofIterable(source)
						.with(StreamBinarySerializer.create(longSerializer())
								.withDefaultBufferSize(1))
						.streamTo(messaging.sendBinaryStream());

				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocket.register();
			} else {
				fail("Test Exception: " + throwable);
			}
		});

		eventloop.run();

		assertEquals(source, future.get());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

}