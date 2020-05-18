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

package io.datakernel.rpc;

import io.datakernel.common.time.Stopwatch;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcRemoteException;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class RpcHelloWorldTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private interface HelloService {
		String hello(String name) throws Exception;
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
				return Promise.ofException((Throwable) e);
			}
			return Promise.of(new HelloResponse(result));
		};
	}

	private static RpcServer createServer(Eventloop eventloop) {
		return RpcServer.create(eventloop)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withHandler(HelloRequest.class, HelloResponse.class, helloServiceRequestHandler(name -> {
					if (name.equals("--")) {
						throw new Exception("Illegal name");
					}
					return "Hello, " + name + "!";
				}))
				.withListenPort(port);
	}

	private static class BlockingHelloClient implements HelloService, AutoCloseable {
		private final Eventloop eventloop;
		private final RpcClient rpcClient;

		public BlockingHelloClient(Eventloop eventloop) throws Exception {
			this.eventloop = eventloop;
			this.rpcClient = RpcClient.create(eventloop)
					.withMessageTypes(HelloRequest.class, HelloResponse.class)
					.withStrategy(server(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port)));

			rpcClient.startFuture().get();
		}

		@Override
		public String hello(String name) throws Exception {
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

		@Override
		public void close() throws Exception {
			rpcClient.stopFuture().get();
		}
	}

	private static final int TIMEOUT = 1500;
	private static int port;
	private RpcServer server;

	@Before
	public void setUp() throws Exception {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		port = getFreePort();
		server = createServer(eventloop);
		server.listen();
		new Thread(eventloop).start();
	}

	@Test
	public void testBlockingCall() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			for (int i = 0; i < 100; i++) {
				assertEquals("Hello, World!", client.hello("World"));
			}
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testAsyncCall() throws Exception {
		int requestCount = 10;

		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			CountDownLatch latch = new CountDownLatch(requestCount);
			for (int i = 0; i < requestCount; i++) {
				String name = "World" + i;
				client.eventloop.execute(() -> client.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
						.whenComplete(latch::countDown)
						.whenComplete(assertComplete(response -> assertEquals("Hello, " + name + "!", response.message))));
			}
			latch.await();
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testBlocking2Clients() throws Exception {
		try (BlockingHelloClient client1 = new BlockingHelloClient(Eventloop.getCurrentEventloop());
			 BlockingHelloClient client2 = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			assertEquals("Hello, John!", client2.hello("John"));
			assertEquals("Hello, World!", client1.hello("World"));
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testBlockingRpcException() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			client.hello("--");
			fail("Exception expected");
		} catch (RpcRemoteException e) {
			assertEquals("java.lang.Exception: Illegal name", e.getMessage());
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testAsync2Clients() throws Exception {
		int requestCount = 10;

		try (BlockingHelloClient client1 = new BlockingHelloClient(Eventloop.getCurrentEventloop());
			 BlockingHelloClient client2 = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			CountDownLatch latch = new CountDownLatch(2 * requestCount);

			for (int i = 0; i < requestCount; i++) {
				String name = "world" + i;
				client1.eventloop.execute(() ->
						client1.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
								.whenComplete(latch::countDown)
								.whenComplete(assertComplete(response -> assertEquals("Hello, " + name + "!", response.message))));
				client2.eventloop.execute(() ->
						client2.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
								.whenComplete(latch::countDown)
								.whenComplete(assertComplete(response -> assertEquals("Hello, " + name + "!", response.message))));
			}
			latch.await();
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	@Ignore("this is not a test but a benchmark, takes a lot of time")
	public void testRejectedRequests() throws Exception {
		int count = 1_000_000;

		try (BlockingHelloClient client = new BlockingHelloClient(Eventloop.getCurrentEventloop())) {
			for (int t = 0; t < 10; t++) {
				AtomicInteger success = new AtomicInteger(0);
				AtomicInteger error = new AtomicInteger(0);
				CountDownLatch latch = new CountDownLatch(count);
				Stopwatch stopwatch = Stopwatch.createStarted();
				for (int i = 0; i < count; i++) {
					client.eventloop.execute(() ->
							client.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest("benchmark"), TIMEOUT)
									.whenComplete(($, e) -> {
										latch.countDown();
										(e == null ? success : error).incrementAndGet();
									}));
				}
				latch.await();
				System.out.printf("%2d: Elapsed %8s rps: %18s (%d/%d [%d])%n",
						t + 1, stopwatch.stop().toString(), count * 1000000.0 / stopwatch.elapsed(MICROSECONDS), success.get(), count, error.get());
			}
		} finally {
			server.closeFuture().get();
		}
	}
}

