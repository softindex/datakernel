package io.global.forum;

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
import io.global.comm.container.TypedRepoNames;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.ot.ForumMetadata;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.launchers.GlobalNodesModule;
import io.global.mustache.DebugMustacheModule;
import io.global.ot.map.MapOperation;
import io.global.ot.server.CommitStorage;
import io.global.ot.service.ContainerModule;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import io.global.session.KvSessionStore;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;

public final class GlobalForumApp extends Launcher {
	public static final String PROPERTIES_FILE = "forum.properties";
	public static final String DEFAULT_SERVER_ID = "Global Forum App";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + File.separator + "global-fs";
	public static final String DEFAULT_OT_STORAGE = System.getProperty("java.io.tmpdir") + File.separator + "global-ot";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";
	public static final String DEFAULT_FORUM_FS_DIR = "global-forum";
	public static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");

	public static final TypedRepoNames DEFAULT_FORUM_REPO_NAMES = TypedRepoNames.create("global-forum")
			.withRepoName(new Key<ChangeValue<ForumMetadata>>() {}, "metadata")
			.withRepoName(new Key<MapOperation<UserId, UserData>>() {}, "users")
			.withRepoName(new Key<MapOperation<UserId, InetAddress>>() {}, "userIps")
			.withRepoName(new Key<MapOperation<String, IpBanState>>() {}, "bans")

			.withRepoName(new Key<MapOperation<String, ThreadMetadata>>() {}, "threads")

			.withRepoPrefix(Key.of(ThreadOperation.class), "threads")

			.withRepoName(new Key<KvSessionStore<UserId>>() {}, "session");

	@Inject
	AsyncHttpServer server;

	@Inject
	GlobalFsDriver fsDriver;

	@Inject
	GlobalKvDriver<String, UserId> kvDriver;

	@Provides
	Config config() {
		return Config.create()
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("fs.storage", DEFAULT_FS_STORAGE)
				.with("ot.storage", DEFAULT_OT_STORAGE)
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESS)
				// .with("appStoreUrl", "http://127.0.0.1:8088")
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
				new ContainerModule<ForumUserContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new GlobalForumModule(DEFAULT_FORUM_FS_DIR, DEFAULT_FORUM_REPO_NAMES),
				new GlobalNodesModule()
						.overrideWith(new LocalNodeCommonModule(DEFAULT_SERVER_ID).rebindExport(CommitStorage.class, Key.of(CommitStorage.class, "stub"))),
				new DebugMustacheModule()
		);
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is now available at " + String.join(", ", server.getHttpAddresses()));
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalForumApp().launch(args);
	}
}
