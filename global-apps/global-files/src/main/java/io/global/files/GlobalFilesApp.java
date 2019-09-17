package io.global.files;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.loader.StaticLoader;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.fs.http.GlobalFsDriverServlet;
import io.global.fs.local.GlobalFsDriver;
import io.global.launchers.GlobalNodesModule;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;

public final class GlobalFilesApp extends Launcher {
	public static final String PROPERTIES_FILE = "globalfs-app.properties";
	public static final String DEFAULT_SERVER_ID = "Global Files";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + '/' + "global-fs";
	public static final String DEFAULT_STATIC_PATH = "/build";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";

	@Inject
	AsyncHttpServer server;

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	AsyncServlet servlet(StaticLoader resourceLoader, GlobalFsDriver fsDriver) {
		return GlobalFsDriverServlet.create(fsDriver)
				.map(GET, "/*", StaticServlet.create(resourceLoader)
						.withMappingNotFoundTo("index.html"));
	}

	@Provides
	StaticLoader staticLoader(Executor executor, Config config) {
		return StaticLoader.ofClassPath(executor, config.get("app.http.staticPath"));
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("app.http")));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("fs.storage", DEFAULT_FS_STORAGE)
				.with("app.http.staticPath", DEFAULT_STATIC_PATH)
				.with("app.http.listenAddresses", DEFAULT_LISTEN_ADDRESS)
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
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
		new GlobalFilesApp().launch(args);
	}
}

