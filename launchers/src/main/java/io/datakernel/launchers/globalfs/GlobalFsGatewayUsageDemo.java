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

import com.google.inject.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.GlobalFsGateway;
import io.global.fs.api.GlobalFsPath;
import io.global.fs.http.HttpDiscoveryService;
import io.global.fs.http.HttpGlobalFsGateway;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.globalfs.GlobalFsConfigConverters.ofRawServerId;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public final class GlobalFsGatewayUsageDemo extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-gatevay-app.properties";

	private static final KeyPair alice, bob;

	static {
		try {
			alice = PrivKey.fromString("IGt2RZdSjXaDLoLZn4DvimyjXRm4QNYSiXSip-uUkjzE").computeKeys();
			bob = PrivKey.fromString("IEioklPt2UgNsAM0UfSFjKMU5JAu0qBm7EMWwoVQG3Wf").computeKeys();
		} catch (ParseException e) {
			throw new UncheckedException(e);
		}
	}

	@Inject
	Eventloop eventloop;

	@Inject
	GlobalFsGateway gateway;

	@Inject
	FsClient storage;

	@Override
	protected Collection<Module> getModules() {
		return asList(ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("app.discoveryService", "localhost:9001")
								.with("app.gatewayAddress", "localhost:7001")
								.with("app.storage", "/tmp/testStorage")
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
					ExecutorService provide() {
						return Executors.newSingleThreadExecutor();
					}

					@Provides
					@Singleton
					FsClient provide(Config config, Eventloop eventloop, ExecutorService executor) {
						return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "app.storage"));
					}

					@Provides
					@Singleton
					AsyncHttpClient provide(Eventloop eventloop) {
						return AsyncHttpClient.create(eventloop);
					}

					@Provides
					@Singleton
					GlobalFsGateway provide(Config config, AsyncHttpClient httpClient) {
						return new HttpGlobalFsGateway(config.get(ofRawServerId(), "app.gatewayAddress"), httpClient);
					}

					@Provides
					@Singleton
					DiscoveryService provideDiscovery(Config config, AsyncHttpClient httpClient) {
						return new HttpDiscoveryService(config.get(ofInetSocketAddress(), "app.discoveryService"), httpClient);
					}
				});
	}

	@Override
	protected void run() throws Exception {
		GlobalFsPath testFile = GlobalFsPath.of(alice, "firstFs", "folder/test.txt");
		eventloop.post(() ->
				gateway.getMetadata(testFile)
						.thenCompose(meta ->
								gateway.uploader(testFile, meta.getSize())
										.accept(ByteBuf.wrapForReading(new byte[]{10}), null))
						.whenComplete(($, e) -> {
							System.out.println("whenComplete: " + $ + ", " + e);
							shutdown();
						}));
		// eventloop.post(() ->
		// 		storage.downloadSerial("test.txt").streamTo(gateway.uploader(GlobalFsPath.of(alice, "firstFs", "folder/test.txt")))
		// 				.thenCompose($ -> gateway.downloader(GlobalFsPath.of(alice, "firstFs", "folder/test.txt")).streamTo(storage.uploadSerial("test2.txt")))
		// 				.thenCompose($ -> storage.downloadSerial("test.txt").toCollector(ByteBufQueue.collector()))
		// 				.thenCompose(original -> storage.downloadSerial("test2.txt").toCollector(ByteBufQueue.collector())
		// 						.whenResult(transfered -> {
		// 							if (!Arrays.equals(original.asArray(), transfered.asArray())) {
		// 								throw new AssertionError("Data is not equal!");
		// 							}
		// 						}))
		// 				.whenComplete(($, e) -> {
		// 					if (e != null) {
		// 						throw new AssertionError(e);
		// 					}
		// 					shutdown();
		// 				}));
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalFsGatewayUsageDemo().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
