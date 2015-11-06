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

package io.datakernel.rpc.client.sender;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage.AbstractRpcMessage;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;
import io.datakernel.rpc.protocol.RpcMessageSerializer;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory;
import io.datakernel.rpc.protocol.stream.RpcStreamProtocolSettings;
import io.datakernel.rpc.server.RequestHandlers;
import io.datakernel.rpc.server.RequestHandlers.RequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.datakernel.async.AsyncCallbacks.closeFuture;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.NioThreadFactory.defaultNioThreadFactory;
import static io.datakernel.rpc.client.sender.RpcRequestSendingStrategies.*;
import static org.junit.Assert.assertEquals;

public class CombinedStrategiesIntegrationTest {

	private interface HelloService {
		String hello(String name) throws Exception;
	}

	private static class HelloServiceImplOne implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello, " + name + "!";
		}
	}

	private static class HelloServiceImplTwo implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello Hello, " + name + "!";
		}
	}

	private static class HelloServiceImplThree implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello Hello Hello, " + name + "!";
		}
	}

	protected static class HelloRequest extends AbstractRpcMessage {
		@Serialize(order = 0)
		public String name;

		public HelloRequest(@Deserialize("name") String name) {
			this.name = name;
		}
	}

	protected static class HelloResponse extends AbstractRpcMessage {
		@Serialize(order = 0)
		public String message;

		public HelloResponse(@Deserialize("message") String message) {
			this.message = message;
		}
	}

	private static RequestHandlers helloServiceRequestHandler(final HelloService helloService) {
		return new RequestHandlers.Builder().put(HelloRequest.class, new RequestHandler<HelloRequest>() {
			@Override
			public void run(HelloRequest request, ResultCallback<RpcMessageData> callback) {
				String result;
				try {
					result = helloService.hello(request.name);
				} catch (Exception e) {
					callback.onException(e);
					return;
				}
				callback.onResult(new HelloResponse(result));
			}
		}).build();
	}

	private static RpcMessageSerializer serializer() {
		return RpcMessageSerializer.builder().addExtraRpcMessageType(HelloRequest.class, HelloResponse.class).build();
	}

	private static RpcServer createServerOne(NioEventloop eventloop, RpcProtocolFactory protocolFactory) {
		return new RpcServer.Builder(eventloop)
				.requestHandlers(helloServiceRequestHandler(new HelloServiceImplOne()))
				.serializer(serializer())
				.protocolFactory(protocolFactory)
				.build()
				.setListenPort(PORT_1);
	}

	private static RpcServer createServerTwo(NioEventloop eventloop, RpcProtocolFactory protocolFactory) {
		return new RpcServer.Builder(eventloop)
				.requestHandlers(helloServiceRequestHandler(new HelloServiceImplTwo()))
				.serializer(serializer())
				.protocolFactory(protocolFactory)
				.build()
				.setListenPort(PORT_2);
	}

	private static RpcServer createServerThree(NioEventloop eventloop, RpcProtocolFactory protocolFactory) {
		return new RpcServer.Builder(eventloop)
				.requestHandlers(helloServiceRequestHandler(new HelloServiceImplThree()))
				.serializer(serializer())
				.protocolFactory(protocolFactory)
				.build()
				.setListenPort(PORT_3);
	}

	private static class HelloClient implements HelloService, Closeable {
		private final NioEventloop eventloop;
		private final RpcClient client;

		public HelloClient(NioEventloop eventloop, RpcProtocolFactory protocolFactory) throws Exception {

			InetSocketAddress address1 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_1);
			InetSocketAddress address2 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_2);
			InetSocketAddress address3 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_3);

			HashFunction<RpcMessageData> hashFunction = new HashFunction<RpcMessageData>() {
				@Override
				public int hashCode(RpcMessageData item) {
					HelloRequest helloRequest = (HelloRequest) item;
					int hash = 0;
					if (helloRequest.name.startsWith("S")) {
						hash = 1;
					}
					return hash;
				}
			};

			List<InetSocketAddress> addresses = ImmutableList.of(address1, address2, address3);

			this.eventloop = eventloop;
			this.client = new RpcClient.Builder(eventloop)
					.addresses(addresses)
					.serializer(serializer())
					.protocolFactory(protocolFactory)
					.requestSenderFactory(
							roundRobin(
									server(address1),
									sharding(hashFunction,
											server(address2),
											server(address3))
							)
					)
					.build();

			final CompletionCallbackFuture connectCompletion = new CompletionCallbackFuture();
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					client.start(connectCompletion);
				}
			});
			connectCompletion.await();
		}

		@Override
		public String hello(String name) throws Exception {
			ResultCallbackFuture<HelloResponse> result = new ResultCallbackFuture<>();
			helloAsync(name, result);
			try {
				return result.get().message;
			} catch (ExecutionException e) {
				throw (Exception)e.getCause();
			}
		}

		public void helloAsync(final String name, final ResultCallback<HelloResponse> callback) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					client.sendRequest(new HelloRequest(name), TIMEOUT, callback);
				}
			});
		}

		@Override
		public void close() throws IOException {
			final CompletionCallbackFuture callback = new CompletionCallbackFuture();
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					client.stop(callback);
				}
			});
			try {
				callback.await();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final int TIMEOUT = 1500;
	private NioEventloop eventloop;
	private RpcServer serverOne;
	private RpcServer serverTwo;
	private RpcServer serverThree;
	private final RpcProtocolFactory protocolFactory = new RpcStreamProtocolFactory(new RpcStreamProtocolSettings());

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = new NioEventloop();
		serverOne = createServerOne(eventloop, protocolFactory);
		serverOne.listen();
		serverTwo = createServerTwo(eventloop, protocolFactory);
		serverTwo.listen();
		serverThree = createServerThree(eventloop, protocolFactory);
		serverThree.listen();
		defaultNioThreadFactory().newThread(eventloop).start();
	}

	@Test
	public void testBlockingCall() throws Exception {
		try (HelloClient client = new HelloClient(eventloop, protocolFactory)) {

			String currentName = "John";
			String currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello, " + currentName + "!", currentResponse);

			currentName = "Winston";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello Hello, " + currentName + "!", currentResponse);

			currentName = "Ann";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello, " + currentName + "!", currentResponse);

			currentName = "Emma";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello Hello, " + currentName + "!", currentResponse);

			currentName = "Lukas";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello, " + currentName + "!", currentResponse);

			currentName = "Sophia"; // name starts with "s", so hash code is different from previous examples
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello Hello Hello, " + currentName + "!", currentResponse);

		} finally {
			serverOne.closeFuture().await();
			serverTwo.closeFuture().await();
			serverThree.closeFuture().await();

		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}

