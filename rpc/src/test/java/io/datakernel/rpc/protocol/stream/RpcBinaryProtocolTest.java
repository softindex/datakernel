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
import io.datakernel.rpc.client.sender.RpcRequestSendingStrategies;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcMessageSerializer;
import io.datakernel.rpc.server.RpcRequestHandlers;
import io.datakernel.rpc.server.RpcRequestHandlers.RequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
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
import static org.junit.Assert.assertEquals;

public class RpcBinaryProtocolTest {
	private static final int LISTEN_PORT = 12345;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	public static class TestRpcRequestMessage {
		private final String request;

		public TestRpcRequestMessage(@Deserialize("request") String request) {
			this.request = request;
		}

		@Serialize(order = 1)
		public String getRequest() {
			return request;
		}
	}

	public static class TestRpcResponseMessage {
		private final String response;

		public TestRpcResponseMessage(@Deserialize("response") String response) {
			this.response = response;
		}

		@Serialize(order = 1)
		public String getResponse() {
			return response;
		}
	}

	private static RpcRequestHandlers createAsyncFunc(final TestService service) {
		return new RpcRequestHandlers.Builder().put(TestRpcRequestMessage.class, new RequestHandler<TestRpcRequestMessage>() {
			@Override
			public void run(final TestRpcRequestMessage request, final ResultCallback<Object> callback) {
				service.call(request.getRequest(), new ResultCallback<String>() {
					@Override
					public void onResult(String result) {
						callback.onResult(new TestRpcResponseMessage(result));
					}

					@Override
					public void onException(Exception exception) {
						callback.onException(exception);
					}
				});
			}
		}).build();
	}

	interface TestService {
		void call(String request, ResultCallback<String> resultCallback);
	}

	private static RpcMessageSerializer buildMessageSerializer() {
		return RpcMessageSerializer.builder()
				.addExtraRpcMessageType(TestRpcRequestMessage.class, TestRpcResponseMessage.class)
				.build();
	}

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test() throws Exception {
		RpcRequestHandlers handlers = createAsyncFunc(new TestService() {
			@Override
			public void call(String request, ResultCallback<String> resultCallback) {
				resultCallback.onResult("Hello, " + request + "!");
			}
		});
		RpcMessageSerializer serializer = buildMessageSerializer();

		final TestRpcRequestMessage testMessage = new TestRpcRequestMessage("Test");

		final NioEventloop eventloop = new NioEventloop();

		RpcStreamProtocolSettings protocolSettings = new RpcStreamProtocolSettings()
				.packetSize(1 << 10, 1 << 16).compression(true);
		final RpcClient client = RpcClient.builder(eventloop)
				.serializer(serializer)
				.requestSendingStrategy(RpcRequestSendingStrategies.firstAvailable(server(address)))
				.protocolFactory(new RpcStreamProtocolFactory(protocolSettings))
				.build();

		final RpcServer server = new RpcServer.Builder(eventloop)
				.serializer(serializer)
				.requestHandlers(handlers)
				.protocolFactory(new RpcStreamProtocolFactory(protocolSettings))
				.build();
		server.setListenAddress(address);
		server.listen();

		final int countRequests = 10;
		final List<TestRpcResponseMessage> results = Lists.newArrayList();
		final ResultCallback<TestRpcResponseMessage> resultsObserver = new ResultCallback<TestRpcResponseMessage>() {
			@Override
			public void onException(Exception exception) {
				onComplete();
				exception.printStackTrace();
			}

			@Override
			public void onResult(TestRpcResponseMessage result) {
				results.add(result);
				if (results.size() == countRequests)
					onComplete();
			}

			public void onComplete() {
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						client.stop();
						server.close();
					}
				});
			}
		};

		final CompletionCallback completionCallback = new CompletionCallback() {
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
		};
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				client.start(completionCallback);
			}
		});

		eventloop.run();
		assertEquals(countRequests, results.size());
		for (int i = 0; i < countRequests; i++) {
			assertEquals("Hello, " + testMessage.getRequest() + "!", results.get(i).getResponse());
		}

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testCompression() {
		int countRequests = 10;
		NioEventloop eventloop = new NioEventloop();
		RpcMessageSerializer serializer = buildMessageSerializer();
		int defaultPacketSize = 1 << 10;
		int maxPacketSize = 1 << 16;

		// client side
		TestRpcRequestMessage testMessage = new TestRpcRequestMessage("Test");
		List<RpcMessage> sourceList = Lists.newArrayList();
		for (int i = 0; i < countRequests; i++) {
			sourceList.add(new RpcMessage(i, testMessage));
		}
		StreamProducer<RpcMessage> client = StreamProducers.ofIterable(eventloop, sourceList);

		StreamLZ4Compressor compressorClient = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamLZ4Decompressor decompressorClient = new StreamLZ4Decompressor(eventloop);
		StreamSerializer<RpcMessage> serializerClient = new StreamBinarySerializer<>(eventloop, serializer.getSerializer(),
				defaultPacketSize, maxPacketSize, 0, false);
		StreamDeserializer<RpcMessage> deserializerClient = new StreamBinaryDeserializer<>(eventloop, serializer.getSerializer(), maxPacketSize);

		// server side
		StreamLZ4Compressor compressorServer = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamLZ4Decompressor decompressorServer = new StreamLZ4Decompressor(eventloop);
		StreamSerializer<RpcMessage> serializerServer = new StreamBinarySerializer<>(eventloop, serializer.getSerializer(),
				defaultPacketSize, maxPacketSize, 0, false);
		StreamDeserializer<RpcMessage> deserializerServer = new StreamBinaryDeserializer<>(eventloop, serializer.getSerializer(), maxPacketSize);

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
			TestRpcRequestMessage data = (TestRpcRequestMessage) resultsData.get(i).getData();
			assertEquals(testMessage.getRequest(), data.getRequest());
		}

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}