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

package io.datakernel.rpc.hello;

import com.google.common.net.InetAddresses;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcRemoteException;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.util.Stopwatch;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.async.AsyncCallbacks.startFuture;
import static io.datakernel.async.AsyncCallbacks.stopFuture;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.NioThreadFactory.defaultNioThreadFactory;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.rpc.protocol.RpcSerializer.rpcSerializer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

public class RpcNioHelloWorldTest {

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

	private static RpcRequestHandler<HelloRequest> helloServiceRequestHandler(final HelloService helloService) {
		return new RpcRequestHandler<HelloRequest>() {
			@Override
			public void run(HelloRequest request, ResultCallback<Object> callback) {
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

	private static RpcServer createServer(NioEventloop eventloop) {
		return RpcServer.create(eventloop, rpcSerializer(HelloRequest.class, HelloResponse.class))
				.on(HelloRequest.class, helloServiceRequestHandler(new HelloService() {
					@Override
					public String hello(String name) throws Exception {
						if (name.equals("--")) {
							throw new Exception("Illegal name");
						}
						return "Hello, " + name + "!";
					}
				}))
				.setListenPort(PORT);
	}

	private static class BlockingHelloClient implements HelloService, AutoCloseable {
		private final NioEventloop eventloop;
		private final RpcClient client;

		public BlockingHelloClient(NioEventloop eventloop) throws Exception {
			this.eventloop = eventloop;
			this.client = RpcClient.create(eventloop, rpcSerializer(HelloRequest.class, HelloResponse.class))
					.strategy(server(new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT)));

			startFuture(client).await();
		}

		@Override
		public String hello(final String name) throws Exception {
			try {
				ResultCallbackFuture<HelloResponse> future = client.sendRequestFuture(new HelloRequest(name), TIMEOUT);
				return future.get().message;
			} catch (ExecutionException e) {
				throw (Exception) e.getCause();
			}
		}

		@Override
		public void close() throws Exception {
			stopFuture(client).await();
		}
	}

	private static final int PORT = 1234, TIMEOUT = 1500;
	private NioEventloop eventloop;
	private RpcServer server;

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = new NioEventloop();
		server = createServer(eventloop);
		server.listen();
		defaultNioThreadFactory().newThread(eventloop).start();
	}

	@Test
	public void testBlockingCall() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {
			for (int i = 0; i < 100; i++) {
				assertEquals("Hello, World!", client.hello("World"));
			}
		} finally {
			server.closeFuture().await();

		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testAsyncCall() throws Exception {
		int count = 1; // amount requests
		final AtomicInteger success = new AtomicInteger();
		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {
			final CountDownLatch latch = new CountDownLatch(count);
			for (int i = 0; i < count; i++) {
				final String name = "World" + i;
				client.eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						client.client.sendRequest(new HelloRequest(name), TIMEOUT, new ResultCallback<HelloResponse>() {
							@Override
							public void onResult(final HelloResponse response) {
								success.incrementAndGet();
								latch.countDown();
								assertEquals("Hello, " + name + "!", response.message);
							}

							@Override
							public void onException(final Exception exception) {
								latch.countDown();
								System.err.println(exception.getMessage());
							}
						});
					}
				});
			}
			latch.await();
		} finally {
			server.closeFuture().await();
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
			server.closeFuture().await();
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
			server.closeFuture().await();
		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testAsync2Clients() throws Exception {
		int count = 10; // amount requests

		try (BlockingHelloClient client1 = new BlockingHelloClient(eventloop);
		     BlockingHelloClient client2 = new BlockingHelloClient(eventloop)) {
			final CountDownLatch latch1 = new CountDownLatch(count);
			final CountDownLatch latch2 = new CountDownLatch(count);

			for (int i = 0; i < count; i++) {
				final String name = "world" + i;
				client1.eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						client1.client.sendRequest(new HelloRequest(name), TIMEOUT, new ResultCallback<HelloResponse>() {
							@Override
							public void onResult(final HelloResponse response) {
								latch1.countDown();
								assertEquals("Hello, " + name + "!", response.message);
							}

							@Override
							public void onException(final Exception exception) {
								latch1.countDown();
								fail(exception.getMessage());
							}
						});
					}
				});
				client2.eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						client2.client.sendRequest(new HelloRequest(name), TIMEOUT, new ResultCallback<HelloResponse>() {
							@Override
							public void onResult(final HelloResponse response) {
								latch2.countDown();
								assertEquals("Hello, " + name + "!", response.message);
							}

							@Override
							public void onException(final Exception exception) {
								latch2.countDown();
								fail(exception.getMessage());
							}
						});
					}
				});
			}
			latch1.await();
			latch2.await();
		} finally {
			server.closeFuture().await();
		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	//@Test
	public void benchmark() throws Exception {
		int count = 2_000_000; // amount requests

		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {
			for (int t = 0; t < 5; t++) {
				final AtomicInteger success = new AtomicInteger(0);
				final AtomicInteger error = new AtomicInteger(0);
				final CountDownLatch latch = new CountDownLatch(count);
				Stopwatch stopwatch = Stopwatch.createUnstarted();
				stopwatch.start();
				for (int i = 0; i < count; i++) {
					client.eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							client.client.sendRequest(new HelloRequest("benchmark"), TIMEOUT, new ResultCallback<HelloResponse>() {
								@Override
								public void onResult(HelloResponse result) {
									latch.countDown();
									success.incrementAndGet();
								}

								@Override
								public void onException(Exception exception) {
									latch.countDown();
									error.incrementAndGet();
								}
							});
						}
					});
				}
				latch.await();
				System.out.println(t + ": Elapsed " + stopwatch.stop().toString() + " rps: " + count * 1000.0 / stopwatch.elapsed(MILLISECONDS)
						+ " (" + success.get() + "/" + count + " [" + error.get() + "])");
			}
		} finally {
			server.closeFuture().await();
		}
	}
}

