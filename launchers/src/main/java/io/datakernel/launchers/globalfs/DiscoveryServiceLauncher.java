/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.datakernel.launchers.globalfs;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.http.DiscoveryServlet;
import io.global.globalfs.local.RuntimeDiscoveryService;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.Initializers.ofHttpServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class DiscoveryServiceLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-discovery-service.properties";

	@Inject
	AsyncHttpServer httpServer;

	public static void main(String[] args) throws Exception {
		new DiscoveryServiceLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}

	@Override
	protected final Collection<com.google.inject.Module> getModules() {
		return Collections.singletonList(override(getBaseModules()).with(getOverrideModules()));
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	private Collection<com.google.inject.Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(9001)))
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					@Singleton
					Eventloop provide(Config config,
							OptionalDependency<ThrottlingController> maybeThrottlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withInspector));
					}

					@Provides
					@Singleton
					DiscoveryService provide() {
						return new RuntimeDiscoveryService();
					}

					@Provides
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, DiscoveryService discoveryService, Config config) {
						return AsyncHttpServer.create(eventloop, DiscoveryServlet.wrap(discoveryService))
								.initialize(ofHttpServer(config.getChild("http")));
					}
				}
		);
	}

	/**
	 * Override this method to override base modules supplied in launcher.
	 */
	protected Collection<com.google.inject.Module> getOverrideModules() {
		return emptyList();
	}
}
