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

package io.datakernel.stream.examples;

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.*;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.net.TcpStreamSocketConnection;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.eventloop.SocketReconnector.reconnect;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static org.junit.Assert.fail;

/**
 * Example 6.
 * Example of using TcpStreamSocketConnection and BinaryProtocol.
 */
public final class BinaryProtocolExample {
	static final Eventloop eventloop = new Eventloop();
	static final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);
	private static final int LISTEN_PORT = 1234;
	private static final InetSocketAddress address =
			new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	/* Subclass of SimpleNioServer with BinaryProtocol, which deserializes received stream
	and streams it to other consumer. */
	public static class BinaryProtocolServer extends AbstractServer {

		public BinaryProtocolServer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected SocketConnection createConnection(SocketChannel socketChannel) {
			final StreamBinaryDeserializer<Integer> streamDeserializer =
					new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10);
			return new TcpStreamSocketConnection(eventloop, socketChannel) {
				@Override
				public void wire(StreamProducer<ByteBuf> socketReader,
				                 StreamConsumer<ByteBuf> socketWriter) {
					socketReader.streamTo(streamDeserializer.getInput());
					streamDeserializer.getOutput().streamTo(consumerToList);
				}
			};
		}
	}

	/* Create connection to server with BinaryProtocol, which serializes some set of integers
	and streams them to server. */
	public static void createClientConnection(final Eventloop eventloop) {
		final StreamBinarySerializer<Integer> streamSerializer =
				new StreamBinarySerializer<>(eventloop, intSerializer(), 1, 10, 0, false);

		reconnect(eventloop, address, defaultSocketSettings(), 3, 100L, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				TcpStreamSocketConnection connection =
						new TcpStreamSocketConnection(eventloop, socketChannel) {
							@Override
							public void wire(StreamProducer<ByteBuf> socketReader,
							                 StreamConsumer<ByteBuf> socketWriter) {
								streamSerializer.getOutput().streamTo(socketWriter);
								StreamProducers.ofIterable(eventloop, Arrays.asList(6, 5, 4, 3, 2))
										.streamTo(streamSerializer.getInput());
							}
						};
				connection.register();
			}

			@Override
			public void onException(Exception exception) {
				fail();
			}
		});
	}

	/* Run the server and client in an event loop. */
	public static void main(String[] args) throws IOException {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			source.add(i);
		}

		BinaryProtocolServer server = new BinaryProtocolServer(eventloop);
		server.setListenAddress(address).acceptOnce();
		server.listen();

		createClientConnection(eventloop);

		eventloop.run();
	}
}
