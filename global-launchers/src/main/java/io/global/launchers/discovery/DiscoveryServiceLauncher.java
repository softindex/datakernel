/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.global.launchers.discovery;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.Inject;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.api.AnnouncementStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.api.SharedKeyStorage;
import io.global.common.discovery.DiscoveryServlet;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.discovery.RemoteFsAnnouncementStorage;
import io.global.common.discovery.RemoteFsSharedKeyStorage;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class DiscoveryServiceLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "discovery-service.properties";

	@Inject
	AsyncHttpServer httpServer;

	@Override
	protected final Collection<Module> getModules() {
		return Collections.singletonList(override(getBaseModules(), getOverrideModules()));
	}

	private Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						ofProperties(PROPERTIES_FILE)
								.override(Config.ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop(Config config) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")));
					}

					@Provides
					DiscoveryService discoveryService(Eventloop eventloop, AnnouncementStorage announcementStorage, SharedKeyStorage sharedKeyStorage) {
						return LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
					}

					@Provides
					AnnouncementStorage announcementStorage(FsClient storage) {
						return new RemoteFsAnnouncementStorage(storage.subfolder("announcements"));
					}

					@Provides
					SharedKeyStorage sharedKeyStorage(FsClient storage) {
						return new RemoteFsSharedKeyStorage(storage.subfolder("keys"));
					}

					@Provides
					FsClient fsClient(Eventloop eventloop, ExecutorService executor, Config config) {
						return LocalFsClient.create(eventloop, config.get(ofPath(), "discovery.storage"))
								.withRevisions();
					}

					@Provides
					DiscoveryServlet discoveryServlet(DiscoveryService discoveryService) {
						return DiscoveryServlet.create(discoveryService);
					}

					@Provides
					AsyncHttpServer httpServer(Eventloop eventloop, DiscoveryServlet servlet, Config config) {
						return AsyncHttpServer.create(eventloop, servlet).initialize(ofHttpServer(config.getChild("http")));
					}

					@Provides
					public ExecutorService executor(Config config) {
						return ConfigConverters.getExecutor(config.getChild("fs.executor"));
					}

				}
		);
	}

	/**
	 * Override this method to override base modules supplied in launcher.
	 */
	protected Collection<Module> getOverrideModules() {
		return emptyList();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new DiscoveryServiceLauncher().launch(args);
	}
}
