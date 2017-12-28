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

import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
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

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test() throws Exception {
		final String testMessage = "Test";

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		final RpcClient client = RpcClient.create(eventloop)
				.withMessageTypes(String.class)
				.withStrategy(server(address));

		final RpcServer server = RpcServer.create(eventloop)
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, request -> Stages.of("Hello, " + request + "!"))
				.withListenAddress(address);
		server.listen();

		final int countRequests = 10;
		final List<String> results = new ArrayList<>();
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
			final ResultObserver resultObserver = new ResultObserver();
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

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testCompression() {
		BufferSerializer<RpcMessage> serializer = SerializerBuilder.create(getSystemClassLoader())
				.withSubclasses(RpcMessage.MESSAGE_TYPES, String.class)
				.build(RpcMessage.class);

		int countRequests = 10;
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		int defaultPacketSize = 1 << 10;
		int maxPacketSize = 1 << 16;

		// client side
		String testMessage = "Test";
		List<RpcMessage> sourceList = new ArrayList<>();
		for (int i = 0; i < countRequests; i++) {
			sourceList.add(RpcMessage.of(i, testMessage));
		}
		StreamProducer<RpcMessage> client = StreamProducers.ofIterable(eventloop, sourceList);

		StreamLZ4Compressor compressorClient = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamLZ4Decompressor decompressorClient = StreamLZ4Decompressor.create(eventloop);
		StreamBinarySerializer<RpcMessage> serializerClient = StreamBinarySerializer.create(eventloop, serializer)
				.withDefaultBufferSize(defaultPacketSize)
				.withMaxMessageSize(maxPacketSize);
		StreamBinaryDeserializer<RpcMessage> deserializerClient = StreamBinaryDeserializer.create(eventloop, serializer);

		// server side
		StreamLZ4Compressor compressorServer = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamLZ4Decompressor decompressorServer = StreamLZ4Decompressor.create(eventloop);
		StreamBinarySerializer<RpcMessage> serializerServer = StreamBinarySerializer.create(eventloop, serializer)
				.withDefaultBufferSize(defaultPacketSize)
				.withMaxMessageSize(maxPacketSize);
		StreamBinaryDeserializer<RpcMessage> deserializerServer = StreamBinaryDeserializer.create(eventloop, serializer);

		StreamConsumerToList<RpcMessage> results = new StreamConsumerToList<>(eventloop);

		client.streamTo(serializerClient.getInput());
		serializerClient.getOutput().streamTo(compressorClient.getInput());
		compressorClient.getOutput().streamTo(decompressorServer.getInput());
		decompressorServer.getOutput().streamTo(deserializerServer.getInput());

		deserializerServer.getOutput().streamTo(serializerServer.getInput());

		serializerServer.getOutput().streamTo(compressorServer.getInput());
		compressorServer.getOutput().streamTo(decompressorClient.getInput());
		decompressorClient.getOutput().streamTo(deserializerClient.getInput());
		deserializerClient.getOutput().streamTo(results);

		eventloop.run();

		List<RpcMessage> resultsData = results.getList();
		assertEquals(countRequests, resultsData.size());
		for (int i = 0; i < countRequests; i++) {
			assertEquals(i, resultsData.get(i).getCookie());
			String data = (String) resultsData.get(i).getData();
			assertEquals(testMessage, data);
		}

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}