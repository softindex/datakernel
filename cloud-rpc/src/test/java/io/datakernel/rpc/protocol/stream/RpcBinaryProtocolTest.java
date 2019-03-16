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

package io.datakernel.rpc.protocol.stream;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.csp.process.ChannelDeserializer;
import io.datakernel.csp.process.ChannelLZ4Compressor;
import io.datakernel.csp.process.ChannelLZ4Decompressor;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.MemSize;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
public final class RpcBinaryProtocolTest {
	private static final int LISTEN_PORT = 12345;

	@Test
	public void test() throws Exception {
		String testMessage = "Test";

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));

		RpcServer server = RpcServer.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, request -> Promise.of("Hello, " + request + "!"))
				.withListenPort(LISTEN_PORT);
		server.listen();

		int countRequests = 10;

		List<String> list = await(client.start()
				.then($ ->
						Promises.toList(IntStream.range(0, countRequests)
								.mapToObj(i -> client.<String, String>sendRequest(testMessage, 1000))))
				.acceptEx(($, e) -> {
					client.stop();
					server.close();
				}));

		assertTrue(list.stream().allMatch(response -> response.equals("Hello, " + testMessage + "!")));
	}

	@Test
	public void testCompression() {
		BinarySerializer<RpcMessage> binarySerializer = SerializerBuilder.create(getSystemClassLoader())
				.withSubclasses(RpcMessage.MESSAGE_TYPES, String.class)
				.build(RpcMessage.class);

		int countRequests = 10;

		String testMessage = "Test";
		List<RpcMessage> sourceList = IntStream.range(0, countRequests).mapToObj(i -> RpcMessage.of(i, testMessage)).collect(toList());

		StreamSupplier<RpcMessage> supplier = StreamSupplier.ofIterable(sourceList)
				.transformWith(ChannelSerializer.create(binarySerializer)
						.withInitialBufferSize(MemSize.of(1))
						.withMaxMessageSize(MemSize.of(64)))
				.transformWith(ChannelLZ4Compressor.createFastCompressor())
				.transformWith(ChannelLZ4Decompressor.create())
				.transformWith(ChannelDeserializer.create(binarySerializer));

		List<RpcMessage> list = await(supplier.toList());
		assertEquals(countRequests, list.size());
		for (int i = 0; i < countRequests; i++) {
			assertEquals(i, list.get(i).getCookie());
			String data = (String) list.get(i).getData();
			assertEquals(testMessage, data);
		}
	}
}
