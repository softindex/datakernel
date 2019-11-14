package io.global.photos;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.launchers.GlobalNodesModule;
import io.global.mustache.DebugMustacheModule;
import io.global.ot.service.ContainerModule;
import io.global.ot.session.UserId;
import io.global.photos.container.GlobalPhotosContainer;
import io.global.photos.container.RepoNames;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;

public class GlobalPhotosApp extends Launcher {
	public static final String PROPERTIES_FILE = "photos.properties";
	public static final String DEFAULT_SERVER_ID = "Global Photos App";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + File.separator + "global-fs";
	public static final String DEFAULT_OT_STORAGE = System.getProperty("java.io.tmpdir") + File.separator + "global-ot";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";
	public static final String DEFAULT_FORUM_FS_DIR = "global-photos";
	public static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");
	public static final RepoNames DEFAULT_FORUM_REPO_NAMES = RepoNames.ofDefault("global-photos");

	@Inject
	AsyncHttpServer server;

	@Inject
	GlobalFsDriver fsDriver;

	@Inject
	GlobalKvDriver<String, UserId> kvDriver;

	public static void main(String[] args) throws Exception {
		new GlobalPhotosApp().launch(args);
	}

	@Provides
	Config config() {
		return Config.create()
				.with("image.upload.limit", "52MB")
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("fs.storage", DEFAULT_FS_STORAGE)
				.with("ot.storage", DEFAULT_OT_STORAGE)
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESS)
				.with("appStoreUrl", "http://127.0.0.1:8088")
				.overrideWith(ofProperties(PROPERTIES_FILE, true))
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
				new GlobalPhotosModule(DEFAULT_FORUM_FS_DIR, DEFAULT_FORUM_REPO_NAMES),
				new ContainerModule<GlobalPhotosContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new GlobalNodesModule()
						.overrideWith(new LocalNodeCommonModule(DEFAULT_SERVER_ID)),
				new DebugMustacheModule()
		);
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is now available at " + String.join(", ", server.getHttpAddresses()));
		awaitShutdown();
	}
}
