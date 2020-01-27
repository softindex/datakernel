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

import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.promise.Promise;
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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.global.launchers.Initializers.sslServerInitializer;
import static io.global.util.Utils.PUB_KEYS_MAP_HEX;

public class DiscoveryServiceLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "discovery-service.properties";
	private static final String BASIC_AUTH_LOGIN = ApplicationSettings.getString(DiscoveryServlet.class, "debug.login", "admin");
	private static final String BASIC_AUTH_PASSWORD = ApplicationSettings.getString(DiscoveryServlet.class, "debug.passoword", "admin");

	@Inject
	AsyncHttpServer httpServer;

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
		return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "discovery.storage"))
				.withRevisions();
	}

	@Provides
	DiscoveryServlet discoveryServlet(DiscoveryService discoveryService) {
		return DiscoveryServlet.create(discoveryService);
	}

	@Provides
	AsyncServlet extendedDiscoveryServlet(DiscoveryServlet discoveryServlet, ExecutorService executor, DiscoveryService discoveryService) {
		return RoutingServlet.create()
				.map("/*", discoveryServlet)
				.map("/debug/*", RoutingServlet.create()
						.map(GET, "/*", StaticServlet.create(StaticLoader.ofClassPath(executor, "/"), "discovery-debug.html"))
						.map(GET, "/api/", request -> discoveryService.findAll()
								.map(pks -> HttpResponse.ok200()
										.withJson(JsonUtils.toJson(PUB_KEYS_MAP_HEX, pks))))
						.then(BasicAuth.decorator("discovery debug", (l, p) -> Promise.of(BASIC_AUTH_LOGIN.equals(l) && BASIC_AUTH_PASSWORD.equals(p))))
				);
	}

	@Provides
	AsyncHttpServer httpServer(Eventloop eventloop, AsyncServlet servlet, ExecutorService executor, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(sslServerInitializer(executor, config.getChild("http")));
	}

	@Provides
	public ExecutorService executor(Config config) {
		return ConfigConverters.getExecutor(config.getChild("fs.executor"));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("fs.executor.corePoolSize", String.valueOf(Runtime.getRuntime().availableProcessors()))
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE))
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {})
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new DiscoveryServiceLauncher().launch(args);
	}
}
