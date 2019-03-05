package io.global.fs.demo;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.*;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.DiscoveryServiceDriver;
import io.global.common.discovery.DiscoveryServiceDriverServlet;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsDriverServlet;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

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
					FsClient provide(Eventloop eventloop, ExecutorService executor, Config config) {
						return LocalFsClient.create(eventloop, config.get(ofPath(), "app.simKeyStoragePath"));
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
					DiscoveryService provideDiscovery(IAsyncHttpClient httpClient, Config config) {
						return HttpDiscoveryService.create(config.get(ofInetSocketAddress(), "app.discovery"), httpClient);
					}

					@Provides
					@Singleton
					AsyncServlet provide(Eventloop eventloop, GlobalFsDriver driver, DiscoveryServiceDriver discoveryDriver, StaticLoader resourceLoader) {
						return MiddlewareServlet.create()
								.with("", new GlobalFsDriverServlet(driver))
								.with("/discovery", new DiscoveryServiceDriverServlet(discoveryDriver))
								.with(GET, "/", SingleResourceStaticServlet.create(eventloop, resourceLoader, "index.html"))
								.with(GET, "/view", SingleResourceStaticServlet.create(eventloop, resourceLoader, "key-view.html"))
								.with(GET, "/announce", SingleResourceStaticServlet.create(eventloop, resourceLoader, "announcement.html"))
								.withFallback(StaticServlet.create(eventloop, resourceLoader, "404.html"));
					}

					@Provides
					@Singleton
					GlobalFsDriver provide(GlobalFsNode node, Config config) {
						return GlobalFsDriver.create(node, CheckpointPosStrategy.of(config.get(ofLong(), "app.checkpointOffset", 16384L)));
					}

					@Provides
					@Singleton
					DiscoveryServiceDriver provide(DiscoveryService discoveryService) {
						return DiscoveryServiceDriver.create(discoveryService);
					}

					@Provides
					@Singleton
					StaticLoader provide(ExecutorService executor, Config config) {
						return StaticLoaders.ofPath(executor, Paths.get(config.get("app.http.staticPath")));
					}

					@Provides
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, Config config, AsyncServlet servlet) {
						return AsyncHttpServer.create(eventloop, servlet)
								.initialize(ofHttpServer(config.getChild("app.http")));
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
