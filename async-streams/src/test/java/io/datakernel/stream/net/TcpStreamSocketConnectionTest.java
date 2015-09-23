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

//public final class TcpStreamSocketConnectionTest {
//	private static final int LISTEN_PORT = 1234;
//	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);
//
//	@Before
//	public void setUp() throws Exception {
//		ByteBufPool.clear();
//		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
//	}
//
//	@Test
//	@Ignore
//	public void test() throws Exception {
//		final List<Integer> source = Lists.newArrayList();
//		for (int i = 0; i < 5; i++) {
//			source.add(i);
//		}
//
//		final NioEventloop eventloop = new NioEventloop();
//
//		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);
//
//		SimpleNioServer server = new SimpleNioServer(eventloop) {
//			@Override
//			protected SocketConnection createConnection(SocketChannel socketChannel) {
//				return new TcpStreamSocketConnection(eventloop, socketChannel) {
//					@Override
//					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
//						StreamBinaryDeserializer<Integer> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10);
//						streamDeserializer.streamTo(consumerToList);
//						socketReader.streamTo(streamDeserializer);
//					}
//				};
//			}
//		};
//		server.setListenAddress(address).acceptOnce();
//		server.listen();
//
//		final StreamBinarySerializer<Integer> streamSerializer = new StreamBinarySerializer<>(eventloop, intSerializer(), 1, 10, 0, false);
//		reconnect(eventloop, address, defaultSocketSettings(), 3, 100L, new ConnectCallback() {
//			@Override
//			public void onConnect(SocketChannel socketChannel) {
//				SocketConnection connection = new TcpStreamSocketConnection(eventloop, socketChannel) {
//					@Override
//					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
//						streamSerializer.streamTo(socketWriter);
//						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer);
//					}
//				};
//				connection.register();
//			}
//
//			@Override
//			public void onException(Exception exception) {
//				fail();
//			}
//		});
//
//		eventloop.run();
//
//		assertEquals(source, consumerToList.getList());
//
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
//	}
//
//	@Test
//	@Ignore
//	public void testLoopback() throws Exception {
//		final List<Integer> source = Lists.newArrayList();
//		for (int i = 0; i < 1; i++) {
//			source.add(i);
//		}
//
//		final NioEventloop eventloop = new NioEventloop();
//
//		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);
//
//		SimpleNioServer server = new SimpleNioServer(eventloop) {
//			@Override
//			protected SocketConnection createConnection(SocketChannel socketChannel) {
//				return new TcpStreamSocketConnection(eventloop, socketChannel) {
//					@Override
//					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
//						socketReader.streamTo(socketWriter);
//					}
//				};
//			}
//		};
//		server.setListenAddress(address).acceptOnce();
//		server.listen();
//
//		final StreamBinarySerializer<Integer> streamSerializer = new StreamBinarySerializer<>(eventloop, intSerializer(), 1, 10, 0, false);
//		final StreamBinaryDeserializer<Integer> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10);
//		reconnect(eventloop, address, defaultSocketSettings(), 3, 100L, new ConnectCallback() {
//			@Override
//			public void onConnect(SocketChannel socketChannel) {
//				SocketConnection connection = new TcpStreamSocketConnection(eventloop, socketChannel) {
//					@Override
//					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
//						streamSerializer.streamTo(socketWriter);
//						socketReader.streamTo(streamDeserializer);
//					}
//				};
//				connection.register();
//				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer);
//				streamDeserializer.streamTo(consumerToList);
//			}
//
//			@Override
//			public void onException(Exception exception) {
//				fail();
//			}
//		});
//
//		eventloop.run();
//
//		assertEquals(source, consumerToList.getList());
//
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
//	}
//
//}
