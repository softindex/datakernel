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
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.ExampleCommonModule;
import io.global.common.KeyPair;
import io.global.db.DbItem;
import io.global.db.GlobalDbDriver;
import io.global.db.GlobalDbNodeImpl;
import io.global.db.api.DbClient;
import io.global.launchers.GlobalNodesModule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public final class GlobalDbDemoApp extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globaldb-app.properties";
	public static final String DEFAULT_SERVER_ID = "DB Demo";

	public static final Consumer<DbItem> DB_ITEM_CONSUMER = dbItem -> {
		System.out.print("Key: " + new String(dbItem.getKey(), UTF_8));
		System.out.println(" Value: " + new String(dbItem.getValue(), UTF_8));
	};

	@Inject
	Eventloop eventloop;

	@Inject
	@Named("alice")
	DbClient alice;

	@Override
	protected Collection<com.google.inject.Module> getModules() {
		return asList(ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("node.serverId", DEFAULT_SERVER_ID)
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				override(new AbstractModule() {
					@Provides
					@Singleton
					GlobalDbDriver provide(GlobalDbNodeImpl node) {
						return GlobalDbDriver.create(node);
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
						return driver.adapt(keys);
					}
				}, new GlobalNodesModule())
						.with(new ExampleCommonModule()));
	}

	@Override
	protected void run() throws Exception {

		List<DbItem> dbItems = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			dbItems.add(DbItem.of(("value_" + i).getBytes(UTF_8), ("data_" + i).getBytes(UTF_8), System.currentTimeMillis()));
		}

		eventloop.post(() ->
				alice.upload("test_table")
						.then(ChannelSupplier.ofIterable(dbItems)::streamTo)
						.whenException(Throwable::printStackTrace)
						.whenResult($ -> System.out.println("Data items have been uploaded to database\nDownloading back..."))
						.then($ -> alice.download("test_table"))
						.then(supplier -> supplier.streamTo(ChannelConsumer.ofConsumer(DB_ITEM_CONSUMER)))
						.whenComplete(($, e) -> shutdown())
		);
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalDbDemoApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
