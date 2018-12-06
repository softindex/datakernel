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

package io.datakernel.examples;

import com.google.inject.*;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.Collection;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static java.util.Arrays.asList;

public class RpcExample extends Launcher {
	private static final int SERVICE_PORT = 34765;

	@Inject
	private RpcClient client;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
					@Provides
					@Singleton
					Eventloop eventloop() {
						return Eventloop.create()
								.withFatalErrorHandler(rethrowOnAnyError());
					}

					@Provides
					@Singleton
					RpcServer rpcServer(Eventloop eventloop) {
						return RpcServer.create(eventloop)
								.withMessageTypes(String.class)
								.withHandler(String.class, String.class, request -> Promise.of("Hello " + request))
								.withListenPort(SERVICE_PORT);
					}

					@Provides
					@Singleton
					RpcClient rpcClient(Eventloop eventloop) {
						return RpcClient.create(eventloop)
								.withMessageTypes(String.class)
								.withStrategy(server(new InetSocketAddress(SERVICE_PORT)));
					}
				}
		);
	}

	@Override
	protected void run() {
		client.sendRequest("World", 1000).whenComplete((res, e) -> {
			if (e != null) {
				System.err.println("Got exception: " + e);
			} else {
				System.out.println("Got result: " + res);
			}
		});
	}

	public static void main(String[] args) throws Exception {
		RpcExample example = new RpcExample();
		example.launch(true, args);
	}
}
