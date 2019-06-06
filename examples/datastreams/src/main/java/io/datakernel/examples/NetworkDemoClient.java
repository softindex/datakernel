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

package io.datakernel.examples;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelDeserializer;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;

/**
 * Demonstrates client ("Server #1" from the picture) which sends some data to other server
 * and receives some computed result.
 * Before running, you should launch {@link NetworkDemoServer} first!
 */
public final class NetworkDemoClient {
	public static final int PORT = 9922;
	static {
		LoggerConfigurer.enableLogging();
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		eventloop.connect(new InetSocketAddress("localhost", PORT), new ConnectCallback() {
			@Override
			public void onConnect(@NotNull SocketChannel socketChannel) {
				AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, null);

				StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
						.transformWith(ChannelSerializer.create(INT_SERIALIZER))
						.streamTo(ChannelConsumer.ofSocket(socket));

				StreamConsumerToList<String> consumer = StreamConsumerToList.create();

				ChannelSupplier.ofSocket(socket)
						.transformWith(ChannelDeserializer.create(UTF8_SERIALIZER))
						.streamTo(consumer);

				consumer.getResult()
						.whenResult(list -> list.forEach(System.out::println));
			}

			@Override
			public void onException(@NotNull Throwable e) {
				System.out.printf("Could not connect to server, make sure it is started: %s\n", e);
			}
		});

		eventloop.run();
	}

}
