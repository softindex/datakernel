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

package io.datakernel.launchers.rpc;

import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.Initializer;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.rpc.Initializers.ofRpcServer;

public abstract class RpcServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "rpc-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	RpcServer rpcServer;

	@Provides
	public Eventloop eventloop(Config config,
			@Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	RpcServer provideRpcServer(Config config, Eventloop eventloop, Initializer<RpcServer> rpcServerInitializer) {
		return RpcServer.create(eventloop)
				.initialize(ofRpcServer(config))
				.initialize(rpcServerInitializer);
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.override(ofClassPathProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				getBusinessLogicModule());
	}

	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		String businessLogicModuleName = System.getProperty(BUSINESS_MODULE_PROP);

		Module businessLogicModule = businessLogicModuleName != null ?
				(Module) Class.forName(businessLogicModuleName).newInstance() :
				new AbstractModule() {
					@Provides
					Initializer<RpcServer> rpcServerInitializer() {
						return server -> server
								.withMessageTypes(String.class)
								.withHandler(String.class, String.class,
										req -> Promise.of("Request: " + req));
					}
				};

		Launcher launcher = new RpcServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return businessLogicModule;
			}
		};

		launcher.launch(args);
	}
}
