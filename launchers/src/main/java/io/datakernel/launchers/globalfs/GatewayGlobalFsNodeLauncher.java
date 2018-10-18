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
import com.google.inject.name.Named;
import io.datakernel.async.EventloopTaskScheduler;
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
import io.global.common.PrivKey;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.CheckpointPositionStrategy;
import io.global.fs.api.GlobalFsGateway;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsGatewayServlet;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.local.GlobalFsGatewayDriver;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.Initializers.ofHttpServer;
import static io.datakernel.launchers.globalfs.GlobalFsConfigConverters.ofCheckpointPositionStrategy;
import static io.datakernel.launchers.GlobalFsInitializers.ofLocalGlobalFsNode;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.Initializers.ofHttpServer;
import static io.datakernel.launchers.globalfs.GlobalFsConfigConverters.ofPrivKey;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class GatewayGlobalFsNodeLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-node.properties";

	private static final int server = Integer.parseInt(System.getProperty("globalfs.testing.server"));

	@Inject
	Eventloop eventloop;

	@Inject
	AsyncHttpServer httpServer;

	@Inject
	@Named("gateway")
	AsyncHttpServer gatewayServer;

	@Inject
	DiscoveryService discoveryService;

	@Inject
	EventloopTaskScheduler fetchScheduler;

	@Override
	protected final Collection<com.google.inject.Module> getModules() {
		return Collections.singletonList(override(getBaseModules()).with(getOverrideModules()));
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
												/* alice(p) = */"IQDgF5F32vMi0aU8hqBmOmyzescMgioB5U_RZbtmDcGIZyEArIGynsVJNr88gsJBTbS9fm93Eif5k1EpMFQK4Hwx4IsAAAAAAAA," +
														/* bob(p) = */"IQC1ILOZnWg5ZHyW9suZvkUNvPOanhme9Ht7oeB5cwcMoyAbXTCPO9NLpdJxwMIteowm5tHV7P7uOCx67s6KPZsLLwAAAAAAAAA")

										// very short latency margin so it will actually do the task each time we call it *testing*
										.with("fetching.latencyMargin", "1 second")
										// type does not really matter in our small example *testing*
										.with("fetching.schedule.type", "period")
										// each 30 seconds it will try to fetch from other masters
										.with("fetching.schedule.value", "30 seconds")

										// address of the node for inter-Global-FS HTTP communication
										.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8000 + server)))

										.with("gateway", Config.create()
												// HTTP gateway for users to use
												.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(7000 + server)))
												// our Alice and Bob
												.with("privateKeys",
														/* alice(s) = */ "IGt2RZdSjXaDLoLZn4DvimyjXRm4QNYSiXSip-uUkjzE," +
																/* bob(s) = */ "IEioklPt2UgNsAM0UfSFjKMU5JAu0qBm7EMWwoVQG3Wf")

												// fixed 32 bytes is extremely small - *testing*
												.with("checkpointPositionStrategy", "fixed")
												.with("checkpointPositionStrategy.offset", "32b")))

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
					ExecutorService provide() {
						return Executors.newSingleThreadExecutor();
					}

					@Provides
					@Singleton
					FsClient provide(Eventloop eventloop, ExecutorService executor, Config config) {
						return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "globalfs.storage"));
					}

					@Provides
					@Singleton
					AsyncHttpClient provide(Eventloop eventloop) {
						return AsyncHttpClient.create(eventloop);
					}

					@Provides
					@Singleton
					GlobalFsGateway provide(Config config, GlobalFsNode node) {
						Set<PrivKey> privateKeys = new HashSet<>(config.get(ofList(ofPrivKey()), "globalfs.gateway.privateKeys"));
						CheckpointPositionStrategy checkpointPositionStrategy = config.get(ofCheckpointPositionStrategy(), "globalfs.gateway.checkpointPositionStrategy");
						return GlobalFsGatewayDriver.create(node, privateKeys, checkpointPositionStrategy);
					}

					@Provides
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, GlobalFsNodeServlet servlet, Config config) {
						return AsyncHttpServer.create(eventloop, servlet)
								.initialize(ofHttpServer(config.getChild("globalfs.http")));
					}

					@Provides
					@Named("gateway")
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, GlobalFsGatewayServlet servlet, Config config) {
						return AsyncHttpServer.create(eventloop, servlet)
								.initialize(ofHttpServer(config.getChild("globalfs.gateway.http")));
					}
				}
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	/**
	 * Override this method to override base modules supplied in launcher.
	 */
	protected Collection<com.google.inject.Module> getOverrideModules() {
		return emptyList();
	}

	public static void main(String[] args) throws Exception {
		// ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("ROOT");
		// logger.setLevel(Level.TRACE);
		new GatewayGlobalFsNodeLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
