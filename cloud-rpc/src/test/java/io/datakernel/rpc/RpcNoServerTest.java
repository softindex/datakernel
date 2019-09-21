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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;

import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.test.TestUtils.getFreePort;
import static org.junit.Assert.assertTrue;

public final class RpcNoServerTest {
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
				.withListenPort(PORT);
	}

	private static final int PORT = getFreePort(), TIMEOUT = 1500;

	@Test
	public void testRpcClientStopBug1() throws Exception {
		doTest(true);
	}

	@Test
	public void testRpcClientStopBug2() throws Exception {
		doTest(false);
	}

	private void doTest(boolean startServerAfterConnectTimeout) throws UnknownHostException, InterruptedException {
		Eventloop eventloopServer = Eventloop.create();
		RpcServer server = createServer(eventloopServer);
		eventloopServer.submit(() -> {
			try {
				server.listen();
			} catch (IOException ignore) {
			}
		});
		Thread serverThread = new Thread(eventloopServer);
		if (!startServerAfterConnectTimeout) {
			serverThread.start();
		}

		Eventloop eventloopClient = Eventloop.create().withCurrentThread();

		RpcClient rpcClient = RpcClient.create(eventloopClient)
				.withMessageTypes(HelloRequest.class, HelloResponse.class)
				.withStrategy(server(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT)))
				.withConnectTimeout(Duration.ofMillis(TIMEOUT));

		try {
			await(rpcClient.start()
					.post()
					.whenComplete(($, e) -> {
						if (e != null) {
							System.err.println(e.getMessage());
							assertTrue(e instanceof ConnectException); // connectTimeout
						}
					})
					.thenEx(($1, $2) -> rpcClient.stop())
					.whenComplete(() -> {
						if (startServerAfterConnectTimeout) {
							serverThread.start();
						}
					})
					.whenComplete(() -> System.err.println("Eventloop: " + eventloopClient))
			);
		} finally {
			eventloopServer.submit(server::close);
			serverThread.join();
		}
	}
}
