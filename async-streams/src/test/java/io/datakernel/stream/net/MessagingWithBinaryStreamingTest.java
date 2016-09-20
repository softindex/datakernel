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

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.google.gson.Gson;
import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.*;
import io.datakernel.eventloop.SimpleServer.SocketHandlerProvider;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.serializer.asm.BufferSerializers.longSerializer;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class MessagingWithBinaryStreamingTest {
	private static final int LISTEN_PORT = 4821;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testPing() throws Exception {
		final Eventloop eventloop = Eventloop.create();

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
						messaging.send(msg, ignoreCompletionCallback());
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

		eventloop.connect(address, new ConnectCallback() {
			void ping(int n, final Messaging<Integer, Integer> messaging) {
				messaging.send(n, ignoreCompletionCallback());

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
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				MessagingWithBinaryStreaming<Integer, Integer> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), Integer.class, new Gson(), Integer.class));
				ping(3, messaging);
				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocket.register();
			}

			@Override
			public void onException(Exception exception) {
				fail("Test Exception: " + exception);
			}
		});

		eventloop.run();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testMessagingDownload() throws Exception {
		final List<Long> source = Lists.newArrayList();
//		for (long i = 0; i < 100; i++) {
//			source.add(i);
//		}

		final Eventloop eventloop = Eventloop.create();

		List<Long> l = new ArrayList<>();
		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop, l);

		SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {
			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.receive(new ReceiveMessageCallback<String>() {
					@Override
					public void onReceive(String msg) {
						assertEquals("start", msg);
						StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer(), 1, 10, 0, false);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
						messaging.sendBinaryStreamFrom(streamSerializer.getOutput(), ignoreCompletionCallback());
					}

					@Override
					public void onReceiveEndOfStream() {

					}

					@Override
					public void onException(Exception e) {
					}
				});

				return messaging;
			}
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.send("start", ignoreCompletionCallback());
				messaging.sendEndOfStream(ignoreCompletionCallback());

				StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer(), 10);
				messaging.receiveBinaryStreamTo(streamDeserializer.getInput(), ignoreCompletionCallback());
				streamDeserializer.getOutput().streamTo(consumerToList);

				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocket.register();
			}

			@Override
			public void onException(Exception e) {
				fail("Test Exception: " + e);
			}
		});

		eventloop.run();
		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = Eventloop.create();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {
			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.receive(new ReceiveMessageCallback<String>() {
					@Override
					public void onReceive(String message) {
						assertEquals("start", message);

						StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer(), 10);
						messaging.receiveBinaryStreamTo(streamDeserializer.getInput(), ignoreCompletionCallback());
						streamDeserializer.getOutput().streamTo(consumerToList);

						messaging.sendEndOfStream(ignoreCompletionCallback());
					}

					@Override
					public void onReceiveEndOfStream() {

					}

					@Override
					public void onException(Exception e) {
					}
				});

				return messaging;
			}
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.send("start", ignoreCompletionCallback());

				StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer(), 1, 10, 0, false);
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				messaging.sendBinaryStreamFrom(streamSerializer.getOutput(), ignoreCompletionCallback());

				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocket.register();
			}

			@Override
			public void onException(Exception e) {
				fail("Test Exception: " + e);
			}
		});

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final boolean[] ack = new boolean[]{false};

		final Eventloop eventloop = Eventloop.create();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {
			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.receive(new ReceiveMessageCallback<String>() {
					@Override
					public void onReceive(String msg) {
						assertEquals("start", msg);

						StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer(), 10);
						streamDeserializer.getOutput().streamTo(consumerToList);
						messaging.receiveBinaryStreamTo(streamDeserializer.getInput(), new CompletionCallback() {
							@Override
							public void onComplete() {
								messaging.send("ack", ignoreCompletionCallback());
								messaging.sendEndOfStream(ignoreCompletionCallback());
							}

							@Override
							public void onException(Exception exception) {
							}
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
			}
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.send("start", ignoreCompletionCallback());

				StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer(), 1, 10, 0, false);
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				messaging.sendBinaryStreamFrom(streamSerializer.getOutput(), ignoreCompletionCallback());

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
			}

			@Override
			public void onException(Exception e) {
				fail("Test Exception: " + e);
			}
		});

		eventloop.run();

		assertEquals(source, consumerToList.getList());
		assertTrue(ack[0]);

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = Eventloop.create();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {
			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.receive(new ReceiveMessageCallback<String>() {
					@Override
					public void onReceive(String msg) {
						assertEquals("start", msg);

						messaging.sendEndOfStream(ignoreCompletionCallback());

						StreamBinaryDeserializer<Long> streamDeserializer = StreamBinaryDeserializer.create(eventloop, longSerializer(), 10);
						messaging.receiveBinaryStreamTo(streamDeserializer.getInput(), ignoreCompletionCallback());

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
			}
		};

		SimpleServer server = SimpleServer.create(eventloop, socketHandlerProvider)
				.withListenAddress(address)
				.withAcceptOnce();

		server.listen();

		eventloop.connect(address, new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
						MessagingWithBinaryStreaming<String, String> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket,
								MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

						messaging.send("start", ignoreCompletionCallback());

						StreamBinarySerializer<Long> streamSerializer = StreamBinarySerializer.create(eventloop, longSerializer(), 1, 10, 0, false);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
						messaging.sendBinaryStreamFrom(streamSerializer.getOutput(), ignoreCompletionCallback());
						asyncTcpSocket.setEventHandler(messaging);
						asyncTcpSocket.register();
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

}