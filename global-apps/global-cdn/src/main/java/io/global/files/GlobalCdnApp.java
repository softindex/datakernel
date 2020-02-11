package io.global.files;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.remotefs.FsClient;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.api.AppDir;
import io.global.common.KeyPair;
import io.global.debug.DebugViewerModule;
import io.global.fs.http.RemoteFsServlet;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.api.KvClient;
import io.global.launchers.GlobalNodesModule;
import io.global.launchers.sync.FsSyncModule;
import io.global.launchers.sync.KvSyncModule;
import io.global.ot.TypedRepoNames;
import io.global.ot.service.*;
import io.global.ot.session.AuthModule;
import io.global.ot.session.UserId;
import io.global.session.KvSessionModule;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.Utils.*;
import static io.global.debug.DebugViewerModule.DebugView.FS;
import static io.global.debug.DebugViewerModule.DebugView.KV;
import static io.global.launchers.Initializers.sslServerInitializer;
import static io.global.ot.OTUtils.REGISTRY;

public final class GlobalCdnApp extends Launcher {
	public static final String PROPERTIES_FILE = "global-cdn.properties";
	public static final String DEFAULT_SERVER_ID = "Global CDN";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + '/' + "global-fs";
	public static final String DEFAULT_STATIC_PATH = "front/build";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";
	private static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");
	public static final String DEFAULT_CDN_FS_DIR = "ApplicationData/global-cdn";
	private static final String SESSION_ID = "CDN_SID";

	@Inject
	AsyncHttpServer server;

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	AsyncServlet servlet(StaticServlet staticServlet, @Named("authorization") RoutingServlet authorizationServlet,
			@Named("FS") AsyncServlet fsServlet, @Named("Download") AsyncServlet downloadServlet, @Named("session") AsyncServletDecorator sessionDecorator,
			GlobalFsDriver driver, @Optional @Named("debug") AsyncServlet debugServlet
	) {
		RoutingServlet routingServlet = RoutingServlet.create()
				.map("/fs/*", sessionDecorator.serve(fsServlet))
				.map(GET,"/fs/download/*", downloadServlet)
				.map(POST, "/fs/deleteBulk", sessionDecorator.serve(bulkDeleteServlet(driver)))
				.map(GET,"/static/*", cachedContent().serve(staticServlet))
				.map(GET,"/*", staticServlet)
				.merge(authorizationServlet);
		if (debugServlet != null) {
			routingServlet.map("/debug/*", debugServlet);
		}
		return routingServlet;
	}

	@Provides
	@ContainerScope
	FsClient fsClient(KeyPair keys, GlobalFsDriver driver) {
		return driver.adapt(keys).subfolder(DEFAULT_CDN_FS_DIR);
	}

	@Provides
	@Named("FS")
	AsyncServlet fsServlet(GlobalFsDriver driver, ContainerManager<?> containerManager) {
		return request -> RemoteFsServlet.create(request.getAttachment(FsUserContainer.class).getFsClient())
				.serve(request);
	}

	@Provides
	@Named("Download")
	AsyncServlet downloadServlet(GlobalFsDriver driver, ContainerManager<?> containerManager) {
		//noinspection ConstantConditions - /download is hardcoded inside RemoteFsServlet
		return request -> RemoteFsServlet.create(request.getAttachment(FsUserContainer.class).getFsClient())
				.getSubtree("/download")
				.serve(request);
	}

	@Provides
	CodecFactory codecFactory() {
		return REGISTRY;
	}

	@Provides
	TypedRepoNames typedRepoNames() {
		return TypedRepoNames.create("global-cdn")
				.withRepoName(new Key<KvClient<String, UserId>>() {}, "session");
	}

	@Provides
	StaticLoader staticLoader(Executor executor, Config config) {
		return StaticLoader.ofPath(executor, config.get(ofPath(), "http.staticPath"));
	}

	@Provides
	StaticServlet staticServlet(StaticLoader resourceLoader) {
		return StaticServlet.create(resourceLoader)
				.withMapping(request -> request.getPath().substring(1))
				.withMappingNotFoundTo("index.html");
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Executor executor, Config config, ContainerServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(sslServerInitializer(executor, config.getChild("http")));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("corePoolSize", String.valueOf(Runtime.getRuntime().availableProcessors()))
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("fs.storage", DEFAULT_FS_STORAGE)
				.with("http.staticPath", DEFAULT_STATIC_PATH)
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESS)
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
				Module.create()
						.bind(FsUserContainer.class).in(ContainerScope.class)
						.bind(Key.of(String.class).named(AppDir.class)).toInstance(DEFAULT_CDN_FS_DIR),
				new KvSessionModule(),
				new ContainerModule<FsUserContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new AuthModule(SESSION_ID),
				new DebugViewerModule(FS, KV),
				override(new GlobalNodesModule(),
						new LocalNodeCommonModule(DEFAULT_SERVER_ID)),
				KvSyncModule.create()
						.withPush()
						.withCatchUp(),
				FsSyncModule.create()
						.withPush()
						.withFetch(DEFAULT_CDN_FS_DIR + "/**")
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalCdnApp().launch(args);
	}
}

