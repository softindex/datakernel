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

package io.datakernel.rpc.client.sender;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.hash.ShardingFunction;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.datakernel.rpc.client.sender.RpcStrategies.*;
import static io.datakernel.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

public final class RpcBlockingTest {
	private static final int PORT_1 = getFreePort();
	private static final int PORT_2 = getFreePort();
	private static final int PORT_3 = getFreePort();
	private static final int TIMEOUT = 1500;

	private Thread thread;

	private RpcServer serverOne;
	private RpcServer serverTwo;
	private RpcServer serverThree;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setUp() throws Exception {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		serverOne = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class, HelloResponse.class,
						helloServiceRequestHandler(new HelloServiceImplOne()))
				.withListenPort(PORT_1);
		serverOne.listen();

		serverTwo = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class, HelloResponse.class,
						helloServiceRequestHandler(new HelloServiceImplTwo()))
				.withListenPort(PORT_2);
		serverTwo.listen();

		serverThree = RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class, HelloResponse.class,
						helloServiceRequestHandler(new HelloServiceImplThree()))
				.withListenPort(PORT_3);
		serverThree.listen();

		thread = new Thread(eventloop);
		thread.start();
	}

	@After
	public void tearDown() throws InterruptedException {
		thread.join();
	}

	@Test
	public void testBlockingCall() throws Exception {
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_1);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_2);
		InetSocketAddress address3 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_3);

		ShardingFunction<HelloRequest> shardingFunction = item -> {
			int shard = 0;
			if (item.name.startsWith("S")) {
				shard = 1;
			}
			return shard;
		};

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withStrategy(
						roundRobin(
								server(address1),
								sharding(shardingFunction, server(address2), server(address3)).withMinActiveSubStrategies(2)));

		client.startFuture().get();

		String currentName;
		String currentResponse;

		currentName = "John";
		currentResponse = blockingRequest(client, currentName);
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

		client.stopFuture().get();

		serverOne.closeFuture().get();
		serverTwo.closeFuture().get();
		serverThree.closeFuture().get();
	}

	private static String blockingRequest(RpcClient rpcClient, String name) throws Exception {
		try {
			return rpcClient.getEventloop().submit(
					() -> rpcClient
							.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT))
					.get()
					.message;
		} catch (ExecutionException e) {
			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - cause is rethrown
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

	private static RpcRequestHandler<HelloRequest, HelloResponse> helloServiceRequestHandler(HelloService helloService) {
		return request -> {
			String result;
			try {
				result = helloService.hello(request.name);
			} catch (Exception e) {
				return Promise.ofException(e);
			}
			return Promise.of(new HelloResponse(result));
		};
	}

}

