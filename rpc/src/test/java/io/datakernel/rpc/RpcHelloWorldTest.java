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

package io.datakernel.rpc;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcRemoteException;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.util.Stopwatch;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.EventloopThreadFactory.defaultEventloopThreadFactory;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.*;

public class RpcHelloWorldTest {

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
				return Stage.ofException((Throwable) e);
			}
			return Stage.of(new HelloResponse(result));
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
				.withListenAddress(new InetSocketAddress("localhost", PORT));
	}

	private static class BlockingHelloClient implements HelloService, AutoCloseable {
		private final Eventloop eventloop;
		private final RpcClient rpcClient;

		public BlockingHelloClient(Eventloop eventloop) throws Exception {
			this.eventloop = eventloop;
			this.rpcClient = RpcClient.create(eventloop)
					.withMessageTypes(HelloRequest.class, HelloResponse.class)
					.withStrategy(server(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT)));

			rpcClient.startFuture().get();
		}

		@Override
		public String hello(String name) throws Exception {
			try {
				return rpcClient.getEventloop().submit(() -> rpcClient
						.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT))
						.toCompletableFuture()
						.get().message;
			} catch (ExecutionException e) {
				throw (Exception) e.getCause();
			}
		}

		@Override
		public void close() throws Exception {
			rpcClient.stopFuture().get();
		}
	}

	private static final int PORT = 1234, TIMEOUT = 1500;
	private Eventloop eventloop;
	private RpcServer server;

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		server = createServer(eventloop);
		server.listen();
		defaultEventloopThreadFactory().newThread(eventloop).start();
	}

	@Test
	public void testBlockingCall() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {
			for (int i = 0; i < 0; i++) {
				assertEquals("Hello, World!", client.hello("World"));
			}
		} finally {
			server.closeFuture().get();

		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testAsyncCall() throws Exception {
		int count = 1; // amount requests
		AtomicInteger success = new AtomicInteger();
		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {
			CountDownLatch latch = new CountDownLatch(count);
			for (int i = 0; i < count; i++) {
				String name = "World" + i;
				client.eventloop.execute(() -> client.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
						.whenComplete((helloResponse, throwable) -> {
							if (throwable != null) {
								System.err.println(throwable.getMessage());
							} else {
								success.incrementAndGet();
								assertEquals("Hello, " + name + "!", helloResponse.message);
							}
							latch.countDown();
						}));
			}
			latch.await();
		} finally {
			server.closeFuture().get();
		}
		assertTrue(success.get() > 0);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testBlocking2Clients() throws Exception {
		try (BlockingHelloClient client1 = new BlockingHelloClient(eventloop);
		     BlockingHelloClient client2 = new BlockingHelloClient(eventloop)) {
			assertEquals("Hello, John!", client2.hello("John"));
			assertEquals("Hello, World!", client1.hello("World"));
		} finally {
			server.closeFuture().get();
		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testBlockingRpcException() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {
			client.hello("--");
			fail("Exception expected");
		} catch (RpcRemoteException e) {
			assertEquals("java.lang.Exception: Illegal name", e.getMessage());
		} finally {
			server.closeFuture().get();
		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testAsync2Clients() throws Exception {
		int count = 10; // amount requests

		try (BlockingHelloClient client1 = new BlockingHelloClient(eventloop);
		     BlockingHelloClient client2 = new BlockingHelloClient(eventloop)) {
			CountDownLatch latch1 = new CountDownLatch(count);
			CountDownLatch latch2 = new CountDownLatch(count);

			for (int i = 0; i < count; i++) {
				String name = "world" + i;
				client1.eventloop.execute(() -> client1.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
						.whenComplete((helloResponse, throwable) -> {
							latch1.countDown();
							if (throwable != null) {
								fail(throwable.getMessage());
							} else {
								assertEquals("Hello, " + name + "!", helloResponse.message);
							}
						}));
				client2.eventloop.execute(() -> client2.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest(name), TIMEOUT)
						.whenComplete((helloResponse, throwable) -> {
							latch2.countDown();
							if (throwable != null) {
								fail(throwable.getMessage());
							} else {
								assertEquals("Hello, " + name + "!", helloResponse.message);
							}
						}));
			}
			latch1.await();
			latch2.await();
		} finally {
			server.closeFuture().get();
		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	//	@Test
	public void testRejectedRequests() throws Exception {
		int count = 1_000_000;

		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {
			for (int t = 0; t < 10; t++) {
				AtomicInteger success = new AtomicInteger(0);
				AtomicInteger error = new AtomicInteger(0);
				CountDownLatch latch = new CountDownLatch(count);
				Stopwatch stopwatch = Stopwatch.createUnstarted();
				stopwatch.start();
				for (int i = 0; i < count; i++) {
					client.eventloop.execute(() -> client.rpcClient.<HelloRequest, HelloResponse>sendRequest(new HelloRequest("benchmark"), TIMEOUT)
							.whenComplete((helloResponse, throwable) -> {
								latch.countDown();
								if (throwable != null) {
									error.incrementAndGet();
								} else {
									success.incrementAndGet();
								}
							}));
				}
				latch.await();
				System.out.println(t + ": Elapsed " + stopwatch.stop().toString() + " rps: " + count * 1000000.0 / stopwatch.elapsed(MICROSECONDS)
						+ " (" + success.get() + "/" + count + " [" + error.get() + "])");
			}
		} finally {
			server.closeFuture().get();
		}
	}
}

