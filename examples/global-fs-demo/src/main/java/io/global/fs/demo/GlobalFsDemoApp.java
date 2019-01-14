/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.*;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.PrivKey;
import io.global.common.PrivateKeyStorage;
import io.global.common.PubKey;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.http.RemoteFsServlet;
import io.global.fs.local.GlobalFsDriver;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofLong;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public final class GlobalFsDemoApp extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-app.properties";

	@Inject
	AsyncHttpServer server;

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
					GlobalFsNode provide(IAsyncHttpClient httpClient, Config config) {
						return HttpGlobalFsNode.create(config.get("app.globalFsId"), httpClient);
					}

					@Provides
					@Singleton
					PrivateKeyStorage providePKS(Config config) {
						return new PrivateKeyStorage(config.getChild("app.keys")
								.getChildren()
								.values()
								.stream()
								.map(cfg -> cfg.get(ofPrivKey(), THIS))
								.collect(toMap(PrivKey::computePubKey, identity())));
					}

					@Provides
					@Singleton
					GlobalFsDriver provide(GlobalFsNode node, PrivateKeyStorage pks, Config config) {
						return GlobalFsDriver.create(node, pks, CheckpointPosStrategy.of(config.get(ofLong(), "app.checkpointOffset", 16384L)));
					}

					@Provides
					@Singleton
					StaticLoader provide(ExecutorService executor) {
						return StaticLoaders.ofPath(executor, Paths.get("src/main/resources/static"));
					}

					@Provides
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, Config config, AsyncServlet servlet) {
						return AsyncHttpServer.create(eventloop, servlet)
								.initialize(ofHttpServer(config.getChild("app.http")));
					}

					@Provides
					@Singleton
					AsyncServlet provide(Eventloop eventloop, GlobalFsDriver driver, StaticLoader resourseLoader) {
						Map<PubKey, PrivKey> keys = driver.getPrivateKeyStorage().getKeys();
						Map<PubKey, RemoteFsServlet> servlets = new HashMap<>();
						return MiddlewareServlet.create()
								.with(GET, "/", request ->
										resourseLoader.getResource("index.html")
												.thenApply(buf1 -> {
													String template = buf1.asString(UTF_8);
													String replaced = template.replace("{}", keys.keySet()
															.stream()
															.map(PubKey::asString)
															.map(s -> "<a class=\"box knownPK\" href=\"/" + s + "\">" + s + "</a>")
															.collect(joining("\n")));
													return HttpResponse.ok200()
															.withBody(ByteBuf.wrapForReading(replaced.getBytes(UTF_8)));
												}))
								.with("/:owner", MiddlewareServlet.create()
										.with(GET, "/", request -> {
											try {
												PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
												if (!keys.containsKey(pubKey)) {
													return Promise.ofException(HttpException.ofCode(404, "No private key stored for given public key"));
												}
												return resourseLoader.getResource("key-view.html")
														.thenApply(buf -> {
															String template = buf.asString(UTF_8);
															String replaced = template.replaceAll("\\{key}", pubKey.asString());
															return HttpResponse.ok200()
																	.withBody(ByteBuf.wrapForReading(replaced.getBytes(UTF_8)));
														});
											} catch (ParseException e) {
												throw new UncheckedException(e);
											}
										}))
								.with("/gateway", MiddlewareServlet.create()
										.withFallback(request -> {
											try {
												PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
												if (!keys.containsKey(pubKey)) {
													return Promise.ofException(HttpException.ofCode(404, "No private key stored for given public key"));
												}
												return servlets
														.computeIfAbsent(pubKey, $ -> RemoteFsServlet.create(driver.gatewayFor(pubKey)))
														.serve(request);
											} catch (ParseException e) {
												throw new UncheckedException(e);
											}
										}))
								.withFallback(StaticServlet.create(eventloop, resourseLoader));
					}
				});
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalFsDemoApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
