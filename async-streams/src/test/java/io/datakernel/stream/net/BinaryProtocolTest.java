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
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SimpleNioServer;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.serializer.asm.BufferSerializers.*;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BinaryProtocolTest {
	private static final int LISTEN_PORT = 4821;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testPing() throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		SimpleNioServer server = new SimpleNioServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10),
						new StreamBinarySerializer<>(eventloop, intSerializer(), 2, 10, 0, false))
						.addHandler(Integer.class, new MessagingHandler<Integer, Integer>() {
							@Override
							public void onMessage(Integer item, Messaging<Integer> messaging) {
								System.out.println(item);
								messaging.sendMessage(item);
							}
						});
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10),
								new StreamBinarySerializer<>(eventloop, intSerializer(), 2, 10, 0, false))
								.addStarter(new MessagingStarter<Integer>() {
									@Override
									public void onStart(Messaging<Integer> messaging) {
										messaging.sendMessage(3);
									}
								})
								.addHandler(Integer.class, new MessagingHandler<Integer, Integer>() {
									@Override
									public void onMessage(Integer item, Messaging<Integer> messaging) {
										if (item > 0) {
											messaging.sendMessage(item - 1);
										} else {
											messaging.shutdown();
										}
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception exception) {
						fail("Test Exception: " + exception);
					}
				}
		);

		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testMessagingDownload() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final NioEventloop eventloop = new NioEventloop();

		List<Long> l = new ArrayList<>();
		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop, l);

		SimpleNioServer server = new SimpleNioServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamBinaryDeserializer<>(eventloop, stringSerializer(), 10),
						new StreamBinarySerializer<>(eventloop, stringSerializer(), 2, 10, 0, false))
						.addHandler(String.class, new MessagingHandler<String, String>() {
							@Override
							public void onMessage(String item, Messaging<String> messaging) {
								assertEquals("start", item);
								messaging.shutdownReader();
								StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
								StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer);
								messaging.write(streamSerializer, new CompletionCallback() {
									@Override
									public void onComplete() {

									}

									@Override
									public void onException(Exception exception) {

									}
								});
							}
						});
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamBinaryDeserializer<>(eventloop, stringSerializer(), 10),
								new StreamBinarySerializer<>(eventloop, stringSerializer(), 2, 10, 0, false))
								.addStarter(new MessagingStarter<String>() {
									@Override
									public void onStart(Messaging<String> messaging) {
										messaging.sendMessage("start");
										messaging.shutdownWriter();
										StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
										messaging.binarySocketReader().streamTo(streamDeserializer);
										streamDeserializer.streamTo(consumerToList);
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();
		assertEquals(source, consumerToList.getList());

		// FIXME: 1 ByteBuf Leak (ByteBuf(16) produced by StreamBinaryDeserializer)
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final NioEventloop eventloop = new NioEventloop();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		SimpleNioServer server = new SimpleNioServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamBinaryDeserializer<>(eventloop, stringSerializer(), 10),
						new StreamBinarySerializer<>(eventloop, stringSerializer(), 2, 10, 0, false))
						.addHandler(String.class, new MessagingHandler<String, String>() {
							@Override
							public void onMessage(String item, Messaging<String> messaging) {
								assertEquals("start", item);

								StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
								messaging.binarySocketReader().streamTo(streamDeserializer);
								streamDeserializer.streamTo(consumerToList);

								messaging.shutdownWriter();
							}
						});
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamBinaryDeserializer<>(eventloop, stringSerializer(), 10),
								new StreamBinarySerializer<>(eventloop, stringSerializer(), 2, 10, 0, false))
								.addStarter(new MessagingStarter<String>() {
									@Override
									public void onStart(Messaging<String> messaging) {
										messaging.sendMessage("start");

										StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
										StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer);
										messaging.write(streamSerializer, new CompletionCallback() {

											@Override
											public void onException(Exception exception) {

											}

											@Override
											public void onComplete() {

											}
										});

										messaging.shutdownReader();
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		// FIXME: 1 ByteBuf Leak (ByteBuf(16) produced by StreamBinaryDeserializer)
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final AtomicBoolean ack = new AtomicBoolean(false);

		final NioEventloop eventloop = new NioEventloop();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		SimpleNioServer server = new SimpleNioServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamBinaryDeserializer<>(eventloop, stringSerializer(), 10),
						new StreamBinarySerializer<>(eventloop, stringSerializer(), 2, 10, 0, false))
						.addHandler(String.class, new MessagingHandler<String, String>() {
							@Override
							public void onMessage(String item, final Messaging<String> messaging) {
								assertEquals("start", item);
								System.out.println("receive start");

								StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
								messaging.binarySocketReader().streamTo(streamDeserializer);
								streamDeserializer.streamTo(consumerToList);

								consumerToList.setCompletionCallback(new CompletionCallback() {
									@Override
									public void onComplete() {
										System.out.println("send ack");
										messaging.sendMessage("ack");
										messaging.shutdown();
									}

									@Override
									public void onException(Exception exception) {
										messaging.shutdown();
									}
								});
							}
						});
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamBinaryDeserializer<>(eventloop, stringSerializer(), 10),
								new StreamBinarySerializer<>(eventloop, stringSerializer(), 2, 10, 0, false))
								.addStarter(new MessagingStarter<String>() {
									@Override
									public void onStart(Messaging<String> messaging) {
										System.out.println("send start");
										messaging.sendMessage("start");

										StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
										StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer);
										messaging.write(streamSerializer, new CompletionCallback() {
											@Override
											public void onComplete() {

											}

											@Override
											public void onException(Exception exception) {

											}
										});
									}
								})
								.addHandler(String.class, new MessagingHandler<String, String>() {
									@Override
									public void onMessage(String item, Messaging<String> messaging) {
										ack.set(true);
										assertEquals("ack", item);
										System.out.println("receive ack");
									}
								});

						connection.register();
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();

		assertEquals(source, consumerToList.getList());
		assertTrue(ack.get());

		// FIXME
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final NioEventloop eventloop = new NioEventloop();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		SimpleNioServer server = new SimpleNioServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamGsonDeserializer<>(eventloop, new Gson(), String.class, 10),
						new StreamGsonSerializer<>(eventloop, new Gson(), String.class, 1, 50, 0))
						.addHandler(String.class, new MessagingHandler<String, String>() {
							@Override
							public void onMessage(String item, Messaging<String> messaging) {
								assertEquals("start", item);

								StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
								messaging.binarySocketReader().streamTo(streamDeserializer);
								streamDeserializer.streamTo(consumerToList);

								messaging.shutdownWriter();
							}
						});
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamGsonDeserializer<>(eventloop, new Gson(), String.class, 10),
								new StreamGsonSerializer<>(eventloop, new Gson(), String.class, 1, 50, 0))
								.addStarter(new MessagingStarter<String>() {
									@Override
									public void onStart(Messaging<String> messaging) {
										messaging.sendMessage("start");

										StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
										StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer);
										messaging.write(streamSerializer, new CompletionCallback() {
											@Override
											public void onComplete() {

											}

											@Override
											public void onException(Exception exception) {

											}
										});

										messaging.shutdownReader();
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		// FIXME: 1 ByteBuf Leak (ByteBuf(16) produced by StreamBinaryDeserializer)
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}