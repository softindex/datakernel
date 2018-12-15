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

package io.global.fs.demo;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.csp.ChannelConsumer;
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
import io.global.common.*;
import io.global.common.api.AnnounceData;
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
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;
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
	@Named("alice")
	KeyPair aliceKeys;

	@Inject
	DiscoveryService discoveryService;

	@Inject
	SignedData<AnnounceData> announceData;

	@Override
	protected Collection<com.google.inject.Module> getModules() {
		return asList(ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
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
						return HttpGlobalFsNode.create(config.get("app.globalFsId"), httpClient);
					}

					@Provides
					@Singleton
					PrivateKeyStorage providePKS(Config config) {
						PrivKey alice = config.get(ofPrivKey(), "app.keys.alice");
						return new PrivateKeyStorage(map(alice.computePubKey(), alice));
					}

					@Provides
					@Singleton
					GlobalFsDriver provide(GlobalFsNode node, PrivateKeyStorage pks, Config config) {
						return GlobalFsDriver.create(node, pks, CheckpointPosStrategy.of(16 * 1024));
					}

					@Provides
					@Singleton
					@Named("alice")
					KeyPair provideAlice(Config config) {
						return config.get(ofPrivKey(), "app.keys.alice").computeKeys();
					}

					@Provides
					@Singleton
					@Named("alice")
					FsClient provideAlice(GlobalFsDriver driver, @Named("alice") KeyPair keys) {
						return driver.gatewayFor(keys.getPubKey());
					}

					@Provides
					@Singleton
					SignedData<AnnounceData> provideAnnounceData(Config config) {
						return SignedData.sign(BinaryDataFormats.REGISTRY.get(AnnounceData.class), AnnounceData.of(System.currentTimeMillis(),
								set(config.get(ofRawServerId(), "app.globalFsId"))), aliceKeys.getPrivKey());
					}
				});
	}

	@Override
	protected void run() throws Exception {
		String testFile = "test.txt";

		eventloop.post(() ->
				discoveryService.announce(aliceKeys.getPubKey(), announceData)
						.thenCompose($ -> storage.upload(testFile))
						.thenCompose(ChannelSupplier.of(wrapUtf8("thats some test data right in that file!\n"))::streamTo)
						.thenCompose($ -> ChannelSupplier.ofPromise(storage.download(testFile))
								.streamTo(ChannelConsumer.ofPromise(alice.upload(testFile))))
						.whenException(Throwable::printStackTrace)
						.whenComplete(($, e) -> shutdown())
		);
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalFsDemoApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
