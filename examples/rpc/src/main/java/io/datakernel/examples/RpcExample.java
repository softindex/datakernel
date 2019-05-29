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

import io.datakernel.async.Promise;
import io.datakernel.di.Inject;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;

public class RpcExample extends Launcher {
	private static final int SERVICE_PORT = 34765;

	@Inject
	private RpcClient client;

	@Inject
	private RpcServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return Eventloop.create()
								.withFatalErrorHandler(rethrowOnAnyError());
					}

					@Provides
					RpcServer rpcServer(Eventloop eventloop) {
						return RpcServer.create(eventloop)
								.withMessageTypes(String.class)
								.withHandler(String.class, String.class, request -> Promise.of("Hello " + request))
								.withListenPort(SERVICE_PORT);
					}

					@Provides
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
		example.launch(args);
	}
}
