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

package io.global.kv.demo;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.ExampleCommonModule;
import io.global.common.KeyPair;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvClient;
import io.global.kv.api.KvItem;
import io.global.launchers.GlobalNodesModule;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;

public final class GlobalKvDemoApp extends Launcher {
	public static final String PROPERTIES_FILE = "globalkv-app.properties";
	public static final String DEFAULT_SERVER_ID = "KV Demo";

	public static final Consumer<KvItem<String, String>> DB_ITEM_CONSUMER = item -> {
		System.out.print("Key: " + item.getKey());
		System.out.println(" Value: " + item.getValue());
	};

	@Inject
	Eventloop eventloop;

	@Inject
	@Named("alice")
	KvClient<String, String> alice;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("node.serverId", DEFAULT_SERVER_ID)
								.override(ofClassPathProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				override(
						combine(
								new AbstractModule() {
									@Provides
									GlobalKvDriver<String, String> provide(GlobalKvNode node) {
										return GlobalKvDriver.create(node, STRING_CODEC, STRING_CODEC);
									}

									@Provides
									@Named("alice")
									KeyPair provideAlice(Config config) {
										return config.get(ofPrivKey(), "app.keys.alice").computeKeys();
									}

									@Provides
									@Named("alice")
									KvClient<String, String> provideAlice(GlobalKvDriver<String, String> driver, @Named("alice") KeyPair keys) {
										return driver.adapt(keys);
									}
								},
								new GlobalNodesModule()),
						new ExampleCommonModule()
				)
		);
	}

	@Override
	protected void run() throws Exception {
		List<KvItem<String, String>> items = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			items.add(new KvItem<>(System.currentTimeMillis(), "value_" + i, "data_" + i));
		}

		eventloop.post(() ->
				alice.upload("test_table")
						.then(ChannelSupplier.ofIterable(items)::streamTo)
						.whenException(Throwable::printStackTrace)
						.whenResult($ -> System.out.println("Data items have been uploaded to database\nDownloading back..."))
						.then($ -> alice.download("test_table"))
						.then(supplier -> supplier.streamTo(ChannelConsumer.ofConsumer(DB_ITEM_CONSUMER)))
						.whenComplete(($, e) -> shutdown())
		);
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalKvDemoApp().launch(args);
	}
}
