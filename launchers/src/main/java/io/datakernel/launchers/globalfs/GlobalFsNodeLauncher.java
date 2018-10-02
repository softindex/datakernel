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

import ch.qos.logback.classic.Level;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.api.NodeFactory;
import io.global.globalfs.http.GlobalFsNodeServlet;
import io.global.globalfs.http.HttpDiscoveryService;
import io.global.globalfs.http.HttpGlobalFsNode;
import io.global.globalfs.local.LocalGlobalFsNode;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.Initializers.ofHttpServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class GlobalFsNodeLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-node.properties";

	private static final int server = 2;

	@Inject
	AsyncHttpServer httpServer;

	public static void main(String[] args) throws Exception {
		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("ROOT");
		logger.setLevel(Level.TRACE);
		new GlobalFsNodeLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
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
								.with("globalfs.discoveryAddress", "localhost:9001")
								.with("globalfs.http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8000 + server)))
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
					DiscoveryService provide(Config config, Eventloop eventloop) {
						return new HttpDiscoveryService(AsyncHttpClient.create(eventloop), config.get(ofInetSocketAddress(), "globalfs.discoveryAddress"));
					}

					@Provides
					@Singleton
					NodeFactory provide(AsyncHttpClient httpClient) {
						return serverId -> new HttpGlobalFsNode(serverId, httpClient);
					}

					@Provides
					@Singleton
					GlobalFsNode provide(Config config, DiscoveryService discoveryService, NodeFactory nodeFactory, FsClient storage) {
						return LocalGlobalFsNode.create(
								new RawServerId(config.get(ofInetSocketAddress(), "globalfs.http.listenAddresses")),
								discoveryService, nodeFactory, storage, () -> Duration.ofSeconds(1));
					}

					@Provides
					@Singleton
					AsyncServlet provide(GlobalFsNode node) {
						return GlobalFsNodeServlet.wrap(node);
					}

					@Provides
					@Singleton
					FsClient provide(Eventloop eventloop, ExecutorService executor) {
						return LocalFsClient.create(eventloop, executor, Paths.get("/tmp/TESTS/server" + server));
					}

					@Provides
					@Singleton
					ExecutorService provice() {
						return Executors.newCachedThreadPool();
					}

					@Provides
					@Singleton
					AsyncHttpClient provide(Eventloop eventloop) {
						return AsyncHttpClient.create(eventloop);
					}

					@Provides
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(ofHttpServer(config.getChild("globalfs.http")));
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
