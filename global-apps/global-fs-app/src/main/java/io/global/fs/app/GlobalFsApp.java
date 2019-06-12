package io.global.fs.app;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.loader.StaticLoader;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsDriverServlet;
import io.global.fs.local.GlobalFsDriver;
import io.global.launchers.GlobalNodesModule;

import java.util.concurrent.ExecutorService;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofLong;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.http.HttpMethod.GET;

public final class GlobalFsApp extends Launcher {
	public static final String PROPERTIES_FILE = "globalfs-app.properties";
	public static final String DEFAULT_SERVER_ID = "Global FS";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + '/' + "global-fs";
	public static final String DEFAULT_STATIC_PATH = "/build";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";

	@Inject
	@Named("App")
	AsyncHttpServer server;

	@Provides
	ExecutorService executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	@Named("App")
	AsyncServlet servlet(GlobalFsDriver driver, StaticLoader resourceLoader) {
		return GlobalFsDriverServlet.create(driver)
				.with(GET, "/*", StaticServlet.create(resourceLoader)
						.withMappingNotFoundTo("index.html"));
	}

	@Provides
	GlobalFsDriver globalFsDriver(GlobalFsNode node, Config config) {
		return GlobalFsDriver.create(node, CheckpointPosStrategy.of(config.get(ofLong(), "app.checkpointOffset", 16384L)));
	}

	@Provides
	StaticLoader staticLoader(Config config) {
		return StaticLoader.ofClassPath(config.get("app.http.staticPath"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("node.serverId", DEFAULT_SERVER_ID)
								.with("fs.storage", DEFAULT_FS_STORAGE)
								.override(Config.create()
										.with("app.http.staticPath", DEFAULT_STATIC_PATH)
										.with("app.http.listenAddresses", DEFAULT_LISTEN_ADDRESS))
								.override(ofClassPathProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				override(
						new GlobalNodesModule(),
						new LocalNodeCommonModule(DEFAULT_SERVER_ID))
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalFsApp().launch(args);
	}
}

