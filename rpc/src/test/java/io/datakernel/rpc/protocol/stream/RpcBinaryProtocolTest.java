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

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcSerializer;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.*;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.rpc.client.sender.RpcRequestSendingStrategies.server;
import static io.datakernel.rpc.protocol.RpcSerializer.serializerFor;
import static org.junit.Assert.assertEquals;

public class RpcBinaryProtocolTest {
	private static final int LISTEN_PORT = 12345;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	interface TestService {
		void call(String request, ResultCallback<String> resultCallback);
	}

	private static RpcSerializer buildMessageSerializer() {
		return serializerFor(String.class);
	}

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test() throws Exception {
		RpcSerializer serializer = buildMessageSerializer();

		final String testMessage = "Test";

		final NioEventloop eventloop = new NioEventloop();

		final RpcClient client = RpcClient.create(eventloop, serializer)
				.strategy(server(address));

		final RpcServer server = RpcServer.create(eventloop, serializer)
				.on(String.class, new RpcRequestHandler<String>() {
					@Override
					public void run(String request, ResultCallback<Object> callback) {
						callback.onResult("Hello, " + request + "!");
					}
				})
				.setListenAddress(address);
		server.listen();

		final int countRequests = 10;
		final List<String> results = Lists.newArrayList();
		final ResultCallback<String> resultsObserver = new ResultCallback<String>() {
			@Override
			public void onException(Exception exception) {
				client.stop();
				server.close();
				exception.printStackTrace();
			}

			@Override
			public void onResult(String result) {
				results.add(result);
				if (results.size() == countRequests) {
					client.stop();
					server.close();
				}
			}

		};

		client.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				for (int i = 0; i < countRequests; i++) {
					client.sendRequest(testMessage, 1000, resultsObserver);
				}
			}

			@Override
			public void onException(Exception e) {
				resultsObserver.onException(e);
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
		int countRequests = 10;
		NioEventloop eventloop = new NioEventloop();
		RpcSerializer serializer = buildMessageSerializer();
		int defaultPacketSize = 1 << 10;
		int maxPacketSize = 1 << 16;

		// client side
		String testMessage = "Test";
		List<RpcMessage> sourceList = Lists.newArrayList();
		for (int i = 0; i < countRequests; i++) {
			sourceList.add(new RpcMessage(i, testMessage));
		}
		StreamProducer<RpcMessage> client = StreamProducers.ofIterable(eventloop, sourceList);

		StreamLZ4Compressor compressorClient = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamLZ4Decompressor decompressorClient = new StreamLZ4Decompressor(eventloop);
		StreamSerializer<RpcMessage> serializerClient = new StreamBinarySerializer<>(eventloop, serializer.createSerializer(),
				defaultPacketSize, maxPacketSize, 0, false);
		StreamDeserializer<RpcMessage> deserializerClient = new StreamBinaryDeserializer<>(eventloop, serializer.createSerializer(), maxPacketSize);

		// server side
		StreamLZ4Compressor compressorServer = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamLZ4Decompressor decompressorServer = new StreamLZ4Decompressor(eventloop);
		StreamSerializer<RpcMessage> serializerServer = new StreamBinarySerializer<>(eventloop, serializer.createSerializer(),
				defaultPacketSize, maxPacketSize, 0, false);
		StreamDeserializer<RpcMessage> deserializerServer = new StreamBinaryDeserializer<>(eventloop, serializer.createSerializer(), maxPacketSize);

		StreamConsumers.ToList<RpcMessage> results = new StreamConsumers.ToList<>(eventloop);

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