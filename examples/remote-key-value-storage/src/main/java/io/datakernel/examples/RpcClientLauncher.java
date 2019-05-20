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

import io.datakernel.di.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Args;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.service.ServiceGraphModule;

import java.util.Collection;

import static java.util.Arrays.asList;

// [START EXAMPLE]
public class RpcClientLauncher extends Launcher {
	@Inject
	private RpcClient client;

	@Inject
	@Args
	private String[] args;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				new RpcClientModule()
		);
	}

	@Override
	protected void run() throws Exception {
		int timeout = 1000;

		if (args.length < 2) {
			throw new RuntimeException("Command line args should be like following 1) --put key value   2) --get key");
		}

		switch (args[0]) {
			case "--put":
				client.<PutRequest, PutResponse>sendRequest(new PutRequest(args[1], args[2]), timeout)
						.whenComplete((response, e) -> {
							if (e == null) {
								System.out.println("put request was made successfully");
								System.out.println("previous value: " + response.getPreviousValue());
							} else {
								e.printStackTrace();
							}
							shutdown();
						});
				break;
			case "--get":
				client.<GetRequest, GetResponse>sendRequest(new GetRequest(args[1]), timeout)
						.whenComplete((response, e) -> {
							if (e == null) {
								System.out.println("get request was made successfully");
								System.out.println("value: " + response.getValue());
							} else {
								e.printStackTrace();
							}
							shutdown();
						});
				break;
			default:
				throw new RuntimeException("Error. You should use --put or --get option");
		}
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		RpcClientLauncher launcher = new RpcClientLauncher();
		launcher.launch(args);
	}
}
// [END EXAMPLE]
