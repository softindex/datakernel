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

package io.datakernel.launchers.globalfs;

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
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.fs.http.GlobalFsNodeServlet;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.Initializers.ofHttpServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class GlobalFsNodeLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-node.properties";

	private static final int server = Integer.parseInt(System.getProperty("globalfs.testing.server"));

	@Inject
	AsyncHttpServer httpServer;

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
								.with("globalfs", Config.create()
										// address of the discovery service
										.with("discoveryService", "localhost:9001")
										// storage path for this node
										.with("storage", "/tmp/TESTS/server" + server)
										// this node manages Alice and Bob
										.with("managedPubKeys",
												/* alice(p) = */"cb78f3ac392aa96ec7a1ba3d1848423097cb5d892638ab297149ea03e9b7ba7d:10d6096aaff36c5b11d5abf063e0499e68e63270ef70d6dc18f0c47566ffdac5," +
														/* bob(p) = */"aed50797fe8950ea25745c5cee391156905033ee4e3f5a2df418f687df78a7f1:784ca80eaa2fc2f643052a7469ec23fa2f72dd9ce248044e34ae986d7ce9ef8d")

										// address of the node for inter-Global-FS HTTP communication
										.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8000 + server))))
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				HttpGlobalFsNodeModule.create(),
				new AbstractModule() {
					@Provides
					@Singleton
					Eventloop provide(Config config, OptionalDependency<ThrottlingController> maybeThrottlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withInspector));
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
					FsClient provide(Eventloop eventloop, ExecutorService executor, Config config) {
						return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "globalfs.storage"));
					}

					@Provides
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, GlobalFsNodeServlet servlet, Config config) {
						return AsyncHttpServer.create(eventloop, servlet)
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

	public static void main(String[] args) throws Exception {
		new GlobalFsNodeLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
