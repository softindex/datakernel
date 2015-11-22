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

package io.datakernel.rpc.example;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcSerializer;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.rpc.client.sender.RpcRequestSendingStrategies.server;
import static io.datakernel.rpc.protocol.RpcSerializer.serializerFor;

/**
 * Here we construct and launch both server and client.
 */
public class CumulativeServiceExample {
	private static final int SERVICE_PORT = 34765;

	public static void main(String[] args) throws IOException {
		final NioEventloop eventloop = new NioEventloop();

		RpcSerializer serializer = serializerFor(String.class);

		final RpcServer server = RpcServer.create(eventloop, serializer)
				.on(String.class, new RpcRequestHandler<String>() {
					@Override
					public void run(String request, ResultCallback<Object> callback) {
						callback.onResult("Hello " + request);
					}
				})
				.setListenPort(SERVICE_PORT);

		final RpcClient client = RpcClient.create(eventloop, serializer)
				.strategy(server(new InetSocketAddress(SERVICE_PORT)))
				.connectTimeoutMillis(500);

		server.listen();
		client.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.sendRequest("World", 1000, new ResultCallback<String>() {
					@Override
					public void onResult(String result) {
						System.out.println("Got result: " + result);
						stopExample();
					}

					@Override
					public void onException(Exception exception) {
						System.err.println("Got exception: " + exception);
						stopExample();
					}
				});
			}

			@Override
			public void onException(Exception exception) {
				System.err.println("Could not start client: " + exception);
				stopExample();
			}

			public void stopExample() {
				client.stop();
				server.close();
			}
		});

		eventloop.run();
	}
}
