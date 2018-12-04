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

package io.global.fs.launchers;

import com.google.inject.*;
import com.google.inject.name.Named;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.PrivKey;
import io.global.common.PrivateKeyStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.fs.launchers.GlobalFsConfigConverters.ofPrivKey;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public final class GlobalFsDemoApp extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-app.properties";

	@Inject
	Eventloop eventloop;

	@Inject
	FsClient storage;

	@Inject
	@Named("alice")
	FsClient alice;

	@Inject
	@Named("bob")
	FsClient bob;

	@Override
	protected Collection<Module> getModules() {
		return asList(ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("app.globalFsId", "http://localhost:9000/fs")
								.with("app.discoveryAddress", "localhost:7000")
								.with("app.storage", "/tmp/testStorage")

								.with("app.keys.alice", "d6577f45e352a16e21a29e8b9fb927b17902332c7f141e51a6265558c6bdd7ef")
								.with("app.keys.bob", "538451a22387ba099222bdbfdeaed63435fde46c724eb3c72e8c64843c339ea1")
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					@Singleton
					Eventloop provide(Config config, OptionalDependency<ThrottlingController> maybeThrottlingController) {
						return Eventloop.create()
								.initialize(Initializers.ofEventloop(config.getChild("eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withInspector));
					}

					@Provides
					@Singleton
					ExecutorService provide(Config config) {
						return getExecutor(config);
					}

					@Provides
					@Singleton
					IAsyncHttpClient provide(Eventloop eventloop) {
						return AsyncHttpClient.create(eventloop);
					}

					@Provides
					@Singleton
					DiscoveryService provideDiscovery(IAsyncHttpClient httpClient, Config config) {
						return HttpDiscoveryService.create(config.get(ofInetSocketAddress(), "app.discoveryAddress"), httpClient);
					}

					@Provides
					@Singleton
					FsClient provide(Config config, Eventloop eventloop, ExecutorService executor) {
						return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "app.storage"));
					}

					@Provides
					@Singleton
					GlobalFsNode provide(IAsyncHttpClient httpClient, Config config) {
						return new HttpGlobalFsNode(httpClient, config.get("app.globalFsId"));
					}

					@Provides
					@Singleton
					PrivateKeyStorage providePKS(Config config) {
						PrivKey alice = config.get(ofPrivKey(), "app.keys.alice");
						PrivKey bob = config.get(ofPrivKey(), "app.keys.bob");
						return new PrivateKeyStorage(map(alice.computePubKey(), alice, bob.computePubKey(), bob));
					}

					@Provides
					@Singleton
					GlobalFsDriver provide(GlobalFsNode node, PrivateKeyStorage pks, Config config) {
						return GlobalFsDriver.create(node, pks, CheckpointPosStrategy.randRange(16 * 1024, 1024 * 1024));
					}

					@Provides
					@Singleton
					@Named("alice")
					FsClient provideAlice(GlobalFsDriver driver, Config config) {
						return driver.gatewayFor(config.get(ofPrivKey(), "app.keys.alice").computePubKey());
					}

					@Provides
					@Singleton
					@Named("bob")
					FsClient provideBob(GlobalFsDriver driver, Config config) {
						return driver.gatewayFor(config.get(ofPrivKey(), "app.keys.bob").computePubKey());
					}
				});
	}

	@Override
	protected void run() throws Exception {
		String testFile = "test.txt";

		storage.upload(testFile)
				.thenCompose(ChannelSupplier.of(wrapUtf8("thats some test data right in that file!\n"))::streamTo)
				.thenCompose($ -> storage.downloader(testFile).streamTo(alice.uploader(testFile)))
				.whenComplete(($, e) -> shutdown());
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalFsDemoApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
