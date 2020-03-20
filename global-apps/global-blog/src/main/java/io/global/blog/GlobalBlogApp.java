package io.global.blog;

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
import io.global.blog.container.BlogUserContainer;
import io.global.blog.ot.BlogMetadata;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.fs.local.GlobalFsDriver;
import io.datakernel.kv.KvClient;
import io.global.launchers.GlobalNodesModule;
import io.global.launchers.sync.FsSyncModule;
import io.global.launchers.sync.KvSyncModule;
import io.global.mustache.MustacheModule;
import io.global.ot.TypedRepoNames;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerModule;
import io.global.ot.session.UserId;
import io.global.ot.value.ChangeValue;
import io.global.session.KvSessionModule;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;
import static io.global.Utils.DEFAULT_SYNC_SCHEDULE_CONFIG;

public final class GlobalBlogApp extends Launcher {
	public static final String PROPERTIES_FILE = "blog.properties";
	public static final String DEFAULT_SERVER_ID = "Global Blog App";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + File.separator + "global-fs";
	public static final String DEFAULT_OT_STORAGE = System.getProperty("java.io.tmpdir") + File.separator + "global-ot";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";
	public static final String DEFAULT_BLOG_FS_DIR = "ApplicationData/global-blog";
	public static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");

	public static final TypedRepoNames DEFAULT_BLOG_REPO_NAMES = TypedRepoNames.create("global-blog")
			.withRepoName(new Key<ChangeValue<BlogMetadata>>() {}, "metadata")
			.withRepoName(new Key<MapOperation<UserId, UserData>>() {}, "users")
			.withRepoName(new Key<MapOperation<UserId, InetAddress>>() {}, "userIps")
			.withRepoName(new Key<MapOperation<String, IpBanState>>() {}, "bans")

			.withRepoName(new Key<MapOperation<String, ThreadMetadata>>() {}, "threads")

			.withRepoPrefix(Key.of(ThreadOperation.class), "thread")

			.withRepoName(new Key<KvClient<String, UserId>>() {}, "session");

	@Inject
	AsyncHttpServer server;

	@Inject
	GlobalFsDriver fsDriver;

	@Provides
	Config config() {
		return Config.create()
				.with("executor.corePoolSize", String.valueOf(Runtime.getRuntime().availableProcessors()))
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("fs.storage", DEFAULT_FS_STORAGE)
				.with("ot.storage", DEFAULT_OT_STORAGE)
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESS)
				.with("appStoreUrl", "http://127.0.0.1:8088")
				.with("kv.catchUp.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.with("kv.push.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.with("fs.fetch.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.with("fs.push.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
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
				new GlobalBlogModule(DEFAULT_BLOG_REPO_NAMES),
				new ContainerModule<BlogUserContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new GlobalNodesModule()
						.overrideWith(new LocalNodeCommonModule(DEFAULT_SERVER_ID)),
				new MustacheModule(),
				new KvSessionModule(),
				KvSyncModule.create()
						.withFetch("global-blog/session")
						.withPush(),
				FsSyncModule.create()
						.withPush()
						.withFetch(DEFAULT_BLOG_FS_DIR + "/**")
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalBlogApp().launch(args);
	}
}
