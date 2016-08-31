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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.*;
import io.datakernel.serializer.asm.BufferSerializers;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@SuppressWarnings("unchecked")
public final class SocketStreamingConnectionTest {
	private static final int LISTEN_PORT = 1234;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test() throws Exception {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				SocketStreamingConnection connection = new SocketStreamingConnection(eventloop, asyncTcpSocket);

				StreamBinaryDeserializer<Integer> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10);
				streamDeserializer.getOutput().streamTo(consumerToList);
				connection.receiveStreamTo(streamDeserializer.getInput(), ignoreCompletionCallback());

				return connection;
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		final StreamBinarySerializer<Integer> streamSerializer = new StreamBinarySerializer<>(eventloop, intSerializer(), 1, 10, 0, false);
		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				SocketStreamingConnection connection = new SocketStreamingConnection(eventloop, asyncTcpSocket);
				connection.sendStreamFrom(streamSerializer.getOutput(), ignoreCompletionCallback());
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				asyncTcpSocket.setEventHandler(connection);
				asyncTcpSocket.register();
			}

			@Override
			public void onException(Exception exception) {
				fail();
			}
		});

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testLoopback() throws Exception {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				SocketStreamingConnection connection = new SocketStreamingConnection(eventloop, asyncTcpSocket);

				StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
				connection.receiveStreamTo(forwarder.getInput(), ignoreCompletionCallback());
				connection.sendStreamFrom(forwarder.getOutput(), ignoreCompletionCallback());

				return connection;
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		final StreamBinarySerializer<Integer> streamSerializer = new StreamBinarySerializer<>(eventloop, intSerializer(), 1, 10, 0, false);
		final StreamBinaryDeserializer<Integer> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10);
		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				SocketStreamingConnection connection = new SocketStreamingConnection(eventloop, asyncTcpSocket);
				connection.sendStreamFrom(streamSerializer.getOutput(), ignoreCompletionCallback());
				connection.receiveStreamTo(streamDeserializer.getInput(), ignoreCompletionCallback());
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				streamDeserializer.getOutput().streamTo(consumerToList);
				asyncTcpSocket.setEventHandler(connection);
				asyncTcpSocket.register();
			}

			@Override
			public void onException(Exception exception) {
				fail();
			}
		});

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Ignore // TODO(vmykhalko): check this test
	@Test
	public void testLoopbackWithError() throws Exception {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		List<Integer> list = new ArrayList<>();
		final TestStreamConsumers.TestConsumerToList<Integer> consumerToListWithError = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (list.size() == 50) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
			}
		};

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				SocketStreamingConnection connection = new SocketStreamingConnection(eventloop, asyncTcpSocket);

				StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
				connection.receiveStreamTo(forwarder.getInput(), ignoreCompletionCallback());
				connection.sendStreamFrom(forwarder.getOutput(), ignoreCompletionCallback());

				return connection;
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		final StreamBinarySerializer<Integer> streamSerializer = new StreamBinarySerializer<>(eventloop, BufferSerializers.intSerializer(), 1, 50, 0, false);
		final StreamBinaryDeserializer<Integer> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, BufferSerializers.intSerializer(), 10);
		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				SocketStreamingConnection connection = new SocketStreamingConnection(eventloop, asyncTcpSocket);
				connection.sendStreamFrom(streamSerializer.getOutput(), ignoreCompletionCallback());
				connection.receiveStreamTo(streamDeserializer.getInput(), ignoreCompletionCallback());
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				streamDeserializer.getOutput().streamTo(consumerToListWithError);
				asyncTcpSocket.setEventHandler(connection);
				asyncTcpSocket.register();
			}

			@Override
			public void onException(Exception exception) {
				fail();
			}
		});

		eventloop.run();

		assertEquals(list.size(), 50);

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
