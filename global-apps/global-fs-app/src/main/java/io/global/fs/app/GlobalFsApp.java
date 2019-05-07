package io.global.fs.app;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsDriverServlet;
import io.global.fs.local.GlobalFsDriver;
import io.global.launchers.GlobalNodesModule;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

public final class GlobalFsApp extends Launcher {
	private static final Logger logger = LoggerFactory.getLogger(GlobalFsApp.class);

	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "globalfs-app.properties";
	public static final String DEFAULT_SERVER_ID = "Global FS";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + '/' + "global-fs";

	@Inject
	@Named("App")
	AsyncHttpServer server;

	@Override
	protected Collection<com.google.inject.Module> getModules() {
		return asList(ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("node.serverId", DEFAULT_SERVER_ID)
								.with("fs.storage", DEFAULT_FS_STORAGE)
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					@Singleton
					ExecutorService provide(Config config) {
						return getExecutor(config);
					}

					@Provides
					@Singleton
					@Named("App")
					AsyncServlet provide(Eventloop eventloop, GlobalFsDriver driver, StaticLoader resourceLoader) {
						return GlobalFsDriverServlet.create(driver)
								.with(GET, "/*", StaticServlet.create(eventloop, resourceLoader, "index.html"));
					}

					@Provides
					@Singleton
					GlobalFsDriver provide(GlobalFsNode node, Config config) {
						return GlobalFsDriver.create(node, CheckpointPosStrategy.of(config.get(ofLong(), "app.checkpointOffset", 16384L)));
					}

					@Provides
					@Singleton
					StaticLoader provide(ExecutorService executor, Config config) {
						return StaticLoaders.ofPath(executor, Paths.get(config.get("app.http.staticPath")));
					}

					@Provides
					@Singleton
					@Named("App")
					AsyncHttpServer provide(Eventloop eventloop, Config config, @Named("App") AsyncServlet servlet) {
						return AsyncHttpServer.create(eventloop, servlet)
								.initialize(ofHttpServer(config.getChild("app.http")));
					}
				},
				override(new GlobalNodesModule())
						.with(new AbstractModule() {
							@Provides
							@Singleton
							DiscoveryService provideDiscoveryService(Eventloop eventloop, Config config, IAsyncHttpClient client) {
								InetSocketAddress discoveryAddress = config.get(ofInetSocketAddress(), "discovery.address", null);
								if (discoveryAddress != null) {
									logger.info("Using remote discovery service at " + discoveryAddress);
									return HttpDiscoveryService.create(discoveryAddress, client);
								}
								logger.warn("No discovery.address config found, using discovery stub");
								PrivKey stubPK = PrivKey.of(BigInteger.ONE);
								AnnouncementStorage announcementStorage = new AnnouncementStorage() {
									@Override
									public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
										throw new UnsupportedOperationException();
									}

									@Override
									public Promise<@Nullable SignedData<AnnounceData>> load(PubKey space) {
										AnnounceData announceData = AnnounceData.of(System.currentTimeMillis(), singleton(new RawServerId(DEFAULT_SERVER_ID)));
										return Promise.of(SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, stubPK));
									}
								};
								InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
								return LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
							}
						}));
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalFsApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}

