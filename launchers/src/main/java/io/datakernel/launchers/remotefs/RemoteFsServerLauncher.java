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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.RemoteFsInitializers.ofRemoteFsServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class RemoteFsServerLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "remotefs-server.properties";

	@Inject
	RemoteFsServer remoteFsServer;

	@Override
	protected final Collection<com.google.inject.Module> getModules() {
		return singletonList(override(getBaseModules()).with(getOverrideModules()));
	}

	private Collection<com.google.inject.Module> getBaseModules() {
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
					@Singleton
					public Eventloop provide(Config config,
							OptionalDependency<ThrottlingController> maybeThrottlingController) {

						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withInspector));
					}

					@Provides
					@Singleton
					RemoteFsServer remoteFsServer(Eventloop eventloop, ExecutorService executor,
							Config config) {
						return RemoteFsServer.create(eventloop, executor, config.get(ofPath(), "remotefs.path"))
								.initialize(ofRemoteFsServer(config.getChild("remotefs")));

					}

					@Provides
					@Singleton
					public ExecutorService provide(Config config) {
						return ConfigConverters.getExecutor(config.getChild("remotefs.executor"));
					}
				}
		);
	}

	protected Collection<com.google.inject.Module> getOverrideModules() {
		return emptyList();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RemoteFsServerLauncher() {};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
