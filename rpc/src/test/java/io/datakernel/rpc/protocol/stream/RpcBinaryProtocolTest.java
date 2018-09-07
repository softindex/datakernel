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

package io.datakernel.rpc.protocol.stream;

import io.datakernel.async.Stage;
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
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static java.lang.ClassLoader.getSystemClassLoader;
import static org.junit.Assert.assertEquals;

public class RpcBinaryProtocolTest {
	private static final int LISTEN_PORT = 12345;
	private static final InetSocketAddress address;

	static {
		try {
			address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), LISTEN_PORT);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws Exception {
		String testMessage = "Test";

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		RpcClient client = RpcClient.create(eventloop)
				.withMessageTypes(String.class)
				.withStrategy(server(address));

		RpcServer server = RpcServer.create(eventloop)
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, request -> Stage.of("Hello, " + request + "!"))
				.withListenAddress(address);
		server.listen();

		int countRequests = 10;
		List<String> results = new ArrayList<>();
		class ResultObserver {

			public void setException(Throwable exception) {
				client.stop().whenComplete(($, throwable) -> {
					if (throwable != null) throw new RuntimeException(throwable);
					System.out.println("Client stopped");
					server.close();
				});
			}

			public void setResult(String result) {
				results.add(result);
				if (results.size() == countRequests) {
					client.stop().whenComplete(($, throwable) -> {
						if (throwable != null) throw new RuntimeException(throwable);
						System.out.println("Client stopped");
						server.close();
					});
				}
			}

		}

		client.start().whenComplete(($, throwable) -> {
			ResultObserver resultObserver = new ResultObserver();
			if (throwable != null) {
				resultObserver.setException(throwable);
			} else {
				for (int i = 0; i < countRequests; i++) {
					client.<String, String>sendRequest(testMessage, 1000).whenComplete((s, throwable1) -> {
						if (throwable1 == null) {
							resultObserver.setResult(s);
						} else {
							resultObserver.setException(throwable1);
						}
					});
				}
			}
		});

		eventloop.run();

		assertEquals(countRequests, results.size());
		for (int i = 0; i < countRequests; i++) {
			assertEquals("Hello, " + testMessage + "!", results.get(i));
		}
	}

	@Test
	public void testCompression() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		BufferSerializer<RpcMessage> bufferSerializer = SerializerBuilder.create(getSystemClassLoader())
				.withSubclasses(RpcMessage.MESSAGE_TYPES, String.class)
				.build(RpcMessage.class);

		int countRequests = 10;

		// client side
		String testMessage = "Test";
		List<RpcMessage> sourceList = new ArrayList<>();
		for (int i = 0; i < countRequests; i++) {
			sourceList.add(RpcMessage.of(i, testMessage));
		}
		StreamProducer<RpcMessage> client = StreamProducer.ofIterable(sourceList);

		SerialLZ4Compressor compressor = SerialLZ4Compressor.createFastCompressor();
		SerialBinarySerializer<RpcMessage> serializer = SerialBinarySerializer.create(bufferSerializer)
				.withInitialBufferSize(MemSize.of(1))
				.withMaxMessageSize(MemSize.of(64));
		SerialBinaryDeserializer<RpcMessage> deserializer = SerialBinaryDeserializer.create(bufferSerializer);

		// server side
		SerialLZ4Decompressor decompressor = SerialLZ4Decompressor.create();

		StreamConsumerToList<RpcMessage> results = StreamConsumerToList.create();

		client.streamTo(serializer);
		serializer.streamTo(compressor);
		compressor.streamTo(decompressor);
		decompressor.streamTo(deserializer);
		deserializer.streamTo(results);

		eventloop.run();

		List<RpcMessage> resultsData = results.getList();
		assertEquals(countRequests, resultsData.size());
		for (int i = 0; i < countRequests; i++) {
			assertEquals(i, resultsData.get(i).getCookie());
			String data = (String) resultsData.get(i).getData();
			assertEquals(testMessage, data);
		}
	}
}
