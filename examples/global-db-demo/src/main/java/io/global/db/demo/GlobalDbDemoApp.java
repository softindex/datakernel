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

package io.global.db.demo;

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
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.db.DbItem;
import io.global.db.GlobalDbDriver;
import io.global.db.api.DbClient;
import io.global.db.api.DbStorage;
import io.global.db.api.GlobalDbNode;
import io.global.db.http.HttpGlobalDbNode;
import io.global.db.stub.RuntimeDbStorageStub;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public final class GlobalDbDemoApp extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globaldb-app.properties";

	public static final Consumer<DbItem> DB_ITEM_CONSUMER = dbItem -> {
		System.out.print("Key: " + new String(dbItem.getKey(), UTF_8));
		System.out.println(" Value: " + new String(dbItem.getValue(), UTF_8));
	};

	@Inject
	Eventloop eventloop;

	@Inject
	@Named("alice")
	DbClient alice;

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
					DbStorage provideStorage(Config config) {
						return new RuntimeDbStorageStub();
					}

					@Provides
					@Singleton
					GlobalDbNode provide(IAsyncHttpClient httpClient, Config config) {
						return HttpGlobalDbNode.create(config.get("app.globalDbId"), httpClient);
					}

					@Provides
					@Singleton
					PrivateKeyStorage providePKS(Config config) {
						PrivKey alice = config.get(ofPrivKey(), "app.keys.alice");
						return new PrivateKeyStorage(map(alice.computePubKey(), alice));
					}

					@Provides
					@Singleton
					GlobalDbDriver provide(GlobalDbNode node, PrivateKeyStorage pks) {
						return GlobalDbDriver.create(node, pks);
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
					DbClient provideAlice(GlobalDbDriver driver, @Named("alice") KeyPair keys) {
						return driver.gatewayFor(keys.getPubKey());
					}

					@Provides
					@Singleton
					SignedData<AnnounceData> provideAnnounceData(Config config) {
						return SignedData.sign(BinaryDataFormats.REGISTRY.get(AnnounceData.class), AnnounceData.of(System.currentTimeMillis(),
								set(config.get(ofRawServerId(), "app.globalDbId"))), aliceKeys.getPrivKey());
					}
				});
	}

	@Override
	protected void run() throws Exception {

		List<DbItem> dbItems = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			dbItems.add(DbItem.of(("value_" + i).getBytes(UTF_8), ("data_" + i).getBytes(UTF_8), System.currentTimeMillis()));
		}

		eventloop.post(() ->
				discoveryService.announce(aliceKeys.getPubKey(), announceData)
						.thenCompose($ -> alice.upload("test_table"))
						.thenCompose(ChannelSupplier.ofIterable(dbItems)::streamTo)
						.whenException(Throwable::printStackTrace)
						.whenResult($ -> System.out.println("Data items has been uploaded to database\nDownloading back..."))
						.thenCompose($ -> alice.download("test_table"))
						.thenCompose(supplier -> supplier.streamTo(ChannelConsumer.ofConsumer(DB_ITEM_CONSUMER)))
						.whenComplete(($, e) -> shutdown())
		);
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalDbDemoApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
