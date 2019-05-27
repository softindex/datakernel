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

package io.datakernel.launchers.remotefs;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.Inject;
import io.datakernel.di.Optional;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.service.ServiceGraphModule;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.remotefs.Initializers.ofRemoteFsServer;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class RemoteFsServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "remotefs-server.properties";

	@Inject
	RemoteFsServer remoteFsServer;

	@Override
	protected final Collection<Module> getModules() {
		return singletonList(override(getBaseModules(), getOverrideModules()));
	}

	private Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),

				ConfigModule.create(() ->
						Config.create()
								.override(Config.ofProperties(PROPERTIES_FILE, true))
								.override(Config.ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					public Eventloop provide(Config config,
							@Optional ThrottlingController throttlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")))
								.initialize(eventloop -> eventloop.withInspector(throttlingController));
					}

					@Provides
					RemoteFsServer remoteFsServer(Eventloop eventloop, ExecutorService executor,
							Config config) {
						return RemoteFsServer.create(eventloop, executor, config.get(ofPath(), "remotefs.path"))
								.initialize(ofRemoteFsServer(config.getChild("remotefs")));

					}

					@Provides
					public ExecutorService provide(Config config) {
						return ConfigConverters.getExecutor(config.getChild("remotefs.executor"));
					}
				}
		);
	}

	protected Collection<Module> getOverrideModules() {
		return emptyList();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RemoteFsServerLauncher() {};
		launcher.launch(args);
	}
}
