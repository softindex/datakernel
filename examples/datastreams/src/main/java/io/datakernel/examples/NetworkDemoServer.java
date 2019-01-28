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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.stream.processor.StreamMapper;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.examples.NetworkDemoClient.PORT;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;

/**
 * Demonstrates server ("Server #2" from the picture) which receives some data from clients
 * computes it somehow and sends back the result.
 */
public final class NetworkDemoServer {

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		eventloop.listen(new InetSocketAddress("localhost", PORT), ServerSocketSettings.create(100), channel -> {
			AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, channel, null);

			try {
				System.out.println("Client connected: " + channel.getRemoteAddress());
			} catch (IOException e) {
				e.printStackTrace();
			}

			ChannelSupplier.ofSocket(socket)
					.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
					.transformWith(StreamMapper.create(x -> x + " times 10 = " + x * 10))
					.transformWith(ChannelSerializer.create(UTF8_SERIALIZER))
					.streamTo(ChannelConsumer.ofSocket(socket));
		});

		System.out.println("Connect to the server by running NetworkDemoClient");

		eventloop.run();
	}
}

