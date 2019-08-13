package io.global.video;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.launchers.GlobalNodesModule;

import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;

public final class GlobalVideoApp extends Launcher {
	public static final String PROPERTIES_FILE = "global-video-app.properties";
	public static final String DEFAULT_SERVER_ID = "Global Video App";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + '/' + "global-fs";
	public static final String DEFAULT_STATIC_PATH = "front/build";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";
	public static final String DEFAULT_VIDEOS_DIR = "global-videos";
	public static final String DEFAULT_VIDEOS_REPO = "global-videos";
	public static final String DEFAULT_COMMENTS_REPO_PREFIX = "global-videos/comments";

	@Inject
	AsyncHttpServer server;

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
				new GlobalVideoModule(
						DEFAULT_VIDEOS_DIR,
						DEFAULT_VIDEOS_REPO,
						DEFAULT_COMMENTS_REPO_PREFIX
				),
				new GlobalNodesModule()
						.overrideWith(new LocalNodeCommonModule(DEFAULT_SERVER_ID))
		);
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is now available at " + String.join(", ", server.getHttpAddresses()));
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalVideoApp().launch(args);
	}
}
