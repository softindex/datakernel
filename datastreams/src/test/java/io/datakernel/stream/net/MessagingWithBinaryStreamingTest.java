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

import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.eventloop.SimpleServer.SocketHandlerProvider;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
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
import java.util.function.BiConsumer;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.asm.BufferSerializers.longSerializer;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
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
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testPing() throws Exception {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {
			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				MessagingWithBinaryStreaming<Integer, Integer> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), Integer.class, new Gson(), Integer.class));
				pong(messaging);
				return messaging;
			}

			void pong(final Messaging<Integer, Integer> messaging) {
				messaging.receive(new ReceiveMessageCallback<Integer>() {
					@Override
					public void onReceive(Integer msg) {
						messaging.send(msg);
						pong(messaging);
					}

					@Override
					public void onReceiveEndOfStream() {
						messaging.close();
					}

					@Override
					public void onException(Exception e) {
						messaging.close();
					}
				});
			}

		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address).whenComplete(new BiConsumer<SocketChannel, Throwable>() {
			void ping(int n, final Messaging<Integer, Integer> messaging) {
				messaging.send(n);

				messaging.receive(new ReceiveMessageCallback<Integer>() {
					@Override
					public void onReceive(Integer msg) {
						if (msg > 0) {
							ping(msg - 1, messaging);
						} else {
							messaging.close();
						}
					}

					@Override
					public void onReceiveEndOfStream() {
						// empty
					}

					@Override
					public void onException(Exception e) {
						messaging.close();
					}
				});
			}

			@Override
			public void accept(SocketChannel socketChannel, Throwable throwable) {
				if (throwable == null) {
					AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					MessagingWithBinaryStreaming<Integer, Integer> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
							MessagingSerializers.ofGson(new Gson(), Integer.class, new Gson(), Integer.class));
					ping(3, messaging);
					asyncTcpSocket.setEventHandler(messaging);
					asyncTcpSocket.register();
				} else {
					fail("Test Exception: " + throwable);
				}
			}
		});

		eventloop.run();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testMessagingDownload() throws Exception {
		final List<Long> source = new ArrayList<>();
//		for (long i = 0; i < 100; i++) {
//			source.add(i);
//		}

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create(eventloop);

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> {
			final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
					MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

			messaging.receive(new ReceiveMessageCallback<String>() {
				@Override
				public void onReceive(String msg) {
					assertEquals("start", msg);
					StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer())
							.withDefaultBufferSize(1);
					StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
					streamSerializer.getOutput().streamTo(messaging.sendBinaryStream());
				}

				@Override
				public void onReceiveEndOfStream() {

				}

				@Override
				public void onException(Exception e) {
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
				MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.send("start");
				messaging.sendEndOfStream();

				StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer());
				messaging.receiveBinaryStream().streamTo(streamDeserializer.getInput());
				streamDeserializer.getOutput().streamTo(consumerToList);

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
		final List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create(eventloop);

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> {
			final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
					MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

			messaging.receive(new ReceiveMessageCallback<String>() {
				@Override
				public void onReceive(String message) {
					assertEquals("start", message);

					StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer());
					messaging.receiveBinaryStream().streamTo(streamDeserializer.getInput());
					streamDeserializer.getOutput().streamTo(consumerToList);

					messaging.sendEndOfStream();
				}

				@Override
				public void onReceiveEndOfStream() {

				}

				@Override
				public void onException(Exception e) {
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
				MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.send("start");

				StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer())
						.withDefaultBufferSize(1);
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				streamSerializer.getOutput().streamTo(messaging.sendBinaryStream());

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
	public void testBinaryMessagingUploadAck() throws Exception {
		final List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final boolean[] ack = new boolean[]{false};

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		final StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create(eventloop);

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> {
			final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
					MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

			messaging.receive(new ReceiveMessageCallback<String>() {
				@Override
				public void onReceive(String msg) {
					assertEquals("start", msg);

					StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer());
					streamDeserializer.getOutput().streamTo(consumerToList);
					StreamProducerWithResult<ByteBuf, Void> producer = messaging.receiveBinaryStream();
					producer.streamTo(streamDeserializer.getInput());
					producer.getResult().thenAccept($ -> {
						messaging.send("ack");
						messaging.sendEndOfStream();
					});
				}

				@Override
				public void onReceiveEndOfStream() {

				}

				@Override
				public void onException(Exception exception) {
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
				final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.send("start");

				StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer())
						.withDefaultBufferSize(1);
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				streamSerializer.getOutput().streamTo(messaging.sendBinaryStream());

				messaging.receive(new ReceiveMessageCallback<String>() {
					@Override
					public void onReceive(String msg) {
						assertEquals("ack", msg);
						messaging.close();
						ack[0] = true;
					}

					@Override
					public void onReceiveEndOfStream() {

					}

					@Override
					public void onException(Exception exception) {

					}
				});

				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocket.register();
			} else {
				fail("Test Exception: " + throwable);
			}
		});

		eventloop.run();

		assertEquals(source, consumerToList.getList());
		assertTrue(ack[0]);

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		final List<Long> source = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		final StreamConsumerToList<Long> consumerToList = StreamConsumerToList.create(eventloop);

		SocketHandlerProvider socketHandlerProvider = asyncTcpSocket -> {
			final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
					MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

			messaging.receive(new ReceiveMessageCallback<String>() {
				@Override
				public void onReceive(String msg) {
					assertEquals("start", msg);

					messaging.sendEndOfStream();

					StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer());
					messaging.receiveBinaryStream().streamTo(streamDeserializer.getInput());

					streamDeserializer.getOutput().streamTo(consumerToList);
				}

				@Override
				public void onReceiveEndOfStream() {

				}

				@Override
				public void onException(Exception exception) {

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
				MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.send("start");

				StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer())
						.withDefaultBufferSize(1);
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				streamSerializer.getOutput().streamTo(messaging.sendBinaryStream());
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

}