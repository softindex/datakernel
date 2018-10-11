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

import ch.qos.logback.classic.Level;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
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
import io.datakernel.util.MemSize;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.PrivKey;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.api.NodeFactory;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpDiscoveryService;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.LocalGlobalFsNode;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.launchers.Initializers.*;
import static io.datakernel.launchers.globalfs.GlobalFsConfigConverters.ofPrivKey;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class GatewayGlobalFsNodeLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-node.properties";

	private static final int server = 2;

	@Inject
	AsyncHttpServer httpServer;

	@Inject
	@Named("gateway")
	AsyncHttpServer gatewayServer;

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
								.with("globalfs.gateway.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(7000 + server)))
								// .with("globalfs.gateway.filesystem", "testFs")
								.with("globalfs.gateway.privateKey", "IGj_NxcCAT_monrbK_8P4ZMdS8Bbi5jd7ZY5iev9R6JC")
								.with("globalfs.gateway.checkpointInterval", "32 bytes")
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
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
					DiscoveryService provide(Config config, Eventloop eventloop) {
						return new HttpDiscoveryService(AsyncHttpClient.create(eventloop), config.get(ofInetSocketAddress(), "globalfs.discoveryAddress"));
					}

					@Provides
					@Singleton
					NodeFactory provide(AsyncHttpClient httpClient) {
						return id -> new HttpGlobalFsNode(id, httpClient);
					}

					@Provides
					@Singleton
					GlobalFsNode provide(Config config, RawServerId id, DiscoveryService discoveryService, NodeFactory nodeFactory, FsClient storage) {
						return LocalGlobalFsNode.create(id, discoveryService, nodeFactory, storage)
								.initialize(ofLocalGlobalFsNode(config.getChild("globalfs.local")));
					}

					@Provides
					@Singleton
					RawServerId provide(Config config) {
						return new RawServerId(config.get(ofInetSocketAddress(), "globalfs.http.listenAddresses"));
					}

					@Provides
					@Singleton
					AsyncServlet provide(GlobalFsNode node) {
						return GlobalFsNodeServlet.wrap(node);
					}

					@Provides
					@Singleton
					AsyncServlet provideGateway(Config config, GlobalFsNode node) {
						List<PrivKey> privateKey = config.get(ofList(ofPrivKey()), "globalfs.gateway.privateKeys");
						MemSize checkpointInterval = config.get(ofMemSize(), "globalfs.gateway.checkpointInterval", MemSize.megabytes(1));
						// return RemoteFsServlet.wrap(new RemoteFsDriver(node, privateKey.computeKeys(), fsName, CheckpointPositionStrategy.fixed(checkpointInterval.toLong())));
						throw new UnsupportedOperationException("not implemented."); // TODO anton: implement
					}

					@Provides
					@Singleton
					FsClient provide(Eventloop eventloop, ExecutorService executor) {
						return LocalFsClient.create(eventloop, executor, Paths.get("/tmp/TESTS/server" + server));
					}

					@Provides
					@Singleton
					ExecutorService provide() {
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

					@Provides
					@Named("gateway")
					@Singleton
					AsyncHttpServer provideGateway(Eventloop eventloop, @Named("gateway") AsyncServlet rootServlet, Config config) {
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

	public static void main(String[] args) throws Exception {
		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("ROOT");
		logger.setLevel(Level.TRACE);
		new GatewayGlobalFsNodeLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
