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

package io.global.ot.demo.client;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.ExampleCommonModule;
import io.global.common.ot.OTCommonModule;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.demo.operations.Operation;

import java.util.function.Function;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.demo.util.Utils.*;

public final class GlobalOTDemoApp extends Launcher {
	public static final String PROPERTIES_FILE = "client.properties";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	public static final String DEFAULT_SERVER_ID = "OT Demo";
	public static final String DEFAULT_RESOURCES = "/build";

	@Inject
	@Named("Example")
	AsyncHttpServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.with("node.serverId", DEFAULT_SERVER_ID)
								.with("resources.path", DEFAULT_RESOURCES)
								.override(Config.ofClassPathProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				override(
						new OTCommonModule<Operation>() {
							@Override
							protected void configure() {
								bind(new Key<StructuredCodec<Operation>>() {}).toInstance(OPERATION_CODEC);
								bind(new Key<Function<Operation, String>>() {}).toInstance(DIFF_TO_STRING);
								bind(new Key<OTSystem<Operation>>() {}).toInstance(createOTSystem());
							}
						},
						new GlobalOTDemoModule()),
				override(new GlobalNodesModule(), new ExampleCommonModule())
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalOTDemoApp().launch(args);
	}

}
