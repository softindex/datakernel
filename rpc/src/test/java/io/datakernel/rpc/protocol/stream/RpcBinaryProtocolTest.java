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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.serial.processor.SerialLZ4Compressor;
import io.datakernel.serial.processor.SerialLZ4Decompressor;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.MemSize;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.IntStream;

import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.test.TestUtils.assertComplete;
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

		client.start()
				.thenCompose($ ->
						Promises.toList(IntStream.range(0, countRequests)
								.mapToObj(i -> client.<String, String>sendRequest(testMessage, 1000))))
				.whenComplete(($, e) -> {
					client.stop();
					server.close();
				})
				.whenComplete(assertComplete(list -> assertTrue(list.stream().allMatch(response -> response.equals("Hello, " + testMessage + "!")))));
	}

	@Test
	public void testCompression() {
		BufferSerializer<RpcMessage> bufferSerializer = SerializerBuilder.create(getSystemClassLoader())
				.withSubclasses(RpcMessage.MESSAGE_TYPES, String.class)
				.build(RpcMessage.class);

		int countRequests = 10;

		String testMessage = "Test";
		List<RpcMessage> sourceList = IntStream.range(0, countRequests).mapToObj(i -> RpcMessage.of(i, testMessage)).collect(toList());

		SerialBinarySerializer.create(bufferSerializer)
				.withInitialBufferSize(MemSize.of(1))
				.withMaxMessageSize(MemSize.of(64))

				.apply(StreamSupplier.ofIterable(sourceList))
				.apply(SerialLZ4Compressor.createFastCompressor())
				.apply(SerialLZ4Decompressor.create())
				.apply(SerialBinaryDeserializer.create(bufferSerializer))
				.toList()
				.whenComplete(assertComplete(list -> {
					assertEquals(countRequests, list.size());
					for (int i = 0; i < countRequests; i++) {
						assertEquals(i, list.get(i).getCookie());
						String data = (String) list.get(i).getData();
						assertEquals(testMessage, data);
					}
				}));
	}
}
