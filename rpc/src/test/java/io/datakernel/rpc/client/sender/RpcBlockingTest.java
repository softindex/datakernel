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

import com.google.common.net.InetAddresses;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.hash.ShardingFunction;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.datakernel.async.AsyncCallbacks.startFuture;
import static io.datakernel.async.AsyncCallbacks.stopFuture;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.EventloopThreadFactory.defaultEventloopThreadFactory;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.rpc.client.sender.RpcStrategies.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RpcBlockingTest {

	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final int TIMEOUT = 1500;

	private Eventloop eventloop;
	private Thread thread;

	private RpcServer serverOne;
	private RpcServer serverTwo;
	private RpcServer serverThree;

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = Eventloop.create();

		serverOne = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandlerFor(HelloRequest.class, helloServiceRequestHandler(new HelloServiceImplOne()))
				.withListenPort(PORT_1);
		serverOne.listen();

		serverTwo = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandlerFor(HelloRequest.class, helloServiceRequestHandler(new HelloServiceImplTwo()))
				.withListenPort(PORT_2);
		serverTwo.listen();

		serverThree = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandlerFor(HelloRequest.class, helloServiceRequestHandler(new HelloServiceImplThree()))
				.withListenPort(PORT_3);
		serverThree.listen();

		thread = defaultEventloopThreadFactory().newThread(eventloop);
		thread.start();
	}

	@After
	public void tearDown() throws InterruptedException {
		thread.join();
	}

	@Test
	public void testBlockingCall() throws Exception {
		InetSocketAddress address1 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_1);
		InetSocketAddress address2 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_2);
		InetSocketAddress address3 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_3);

		ShardingFunction<HelloRequest> shardingFunction = new ShardingFunction<HelloRequest>() {
			@Override
			public int getShard(HelloRequest item) {
				int shard = 0;
				if (item.name.startsWith("S")) {
					shard = 1;
				}
				return shard;
			}
		};

		RpcClient client = RpcClient.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withStrategy(
						roundRobin(
								server(address1),
								sharding(shardingFunction, server(address2), server(address3)).withMinActiveSubStrategies(2)));

		startFuture(client).await();

		String currentName = "John";
		String currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello, " + currentName + "!", currentResponse);

		currentName = "Winston";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello Hello, " + currentName + "!", currentResponse);

		currentName = "Ann";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello, " + currentName + "!", currentResponse);

		currentName = "Emma";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello Hello, " + currentName + "!", currentResponse);

		currentName = "Lukas";
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello, " + currentName + "!", currentResponse);

		currentName = "Sophia"; // name starts with "s", so hash code is different from previous examples
		currentResponse = blockingRequest(client, currentName);
		System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
		assertEquals("Hello Hello Hello, " + currentName + "!", currentResponse);

		stopFuture(client).await();

		serverOne.closeFuture().await();
		serverTwo.closeFuture().await();
		serverThree.closeFuture().await();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	private static String blockingRequest(RpcClient rpcClient, final String name) throws Exception {
		try {
			ResultCallbackFuture<HelloResponse> future = rpcClient.sendRequestFuture(new HelloRequest(name), TIMEOUT);
			return future.get().message;
		} catch (ExecutionException e) {
			throw (Exception) e.getCause();
		}
	}

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

	protected static class HelloRequest {
		@Serialize(order = 0)
		public String name;

		public HelloRequest(@Deserialize("name") String name) {
			this.name = name;
		}
	}

	protected static class HelloResponse {
		@Serialize(order = 0)
		public String message;

		public HelloResponse(@Deserialize("message") String message) {
			this.message = message;
		}
	}

	private static RpcRequestHandler<HelloRequest, HelloResponse> helloServiceRequestHandler(final HelloService helloService) {
		return new RpcRequestHandler<HelloRequest, HelloResponse>() {
			@Override
			public void run(HelloRequest request, ResultCallback<HelloResponse> callback) {
				String result;
				try {
					result = helloService.hello(request.name);
				} catch (Exception e) {
					callback.onException(e);
					return;
				}
				callback.onResult(new HelloResponse(result));
			}
		};
	}

}

