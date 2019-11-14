package io.global.files;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
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
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.common.PrivKey;
import io.global.fs.http.GlobalFsDriverServlet;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.GlobalKvDriver;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.ContainerServlet;
import io.global.ot.service.SimpleUserContainer;
import io.global.ot.session.AuthModule;
import io.global.ot.session.UserId;
import io.global.session.KvSessionStore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.ot.OTUtils.REGISTRY;

public final class GlobalFilesApp extends Launcher {
	public static final String PROPERTIES_FILE = "global-files.properties";
	public static final String DEFAULT_SERVER_ID = "Global Files";
	public static final String DEFAULT_FS_STORAGE = System.getProperty("java.io.tmpdir") + '/' + "global-fs";
	public static final String DEFAULT_STATIC_PATH = "/build";
	public static final String DEFAULT_LISTEN_ADDRESS = "8080";
	private static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");
	private static final String FILES_SESSION_TABLE = "files/session";
	private static final String SESSION_ID = "FILES_SID";

	@Inject
	AsyncHttpServer server;

	@Inject
	GlobalKvDriver<String, UserId> kvDriver;

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	AsyncServlet servlet(StaticLoader resourceLoader, GlobalFsDriver fsDriver,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator) {
		return RoutingServlet.create()
				.map("/fs/*", sessionDecorator.serve(GlobalFsDriverServlet.create(fsDriver)))
				.map("/*", StaticServlet.create(resourceLoader).withMappingNotFoundTo("index.html"))
				.merge(authorizationServlet);
	}

	@Provides
	CodecFactory codecFactory() {
		return REGISTRY;
	}

	@Provides
	StaticLoader staticLoader(Executor executor, Config config) {
		return StaticLoader.ofClassPath(executor, config.get("app.http.staticPath"));
	}

	@Provides
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, Config config, ContainerServlet servlet) {
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

	@Provides
	@ContainerScope
	SimpleUserContainer userContainer(Eventloop eventloop, PrivKey privKey, GlobalKvDriver<String, UserId> kvDriver) {
		KvSessionStore<UserId> sessionStore = KvSessionStore.create(eventloop, kvDriver.adapt(privKey), FILES_SESSION_TABLE);
		return SimpleUserContainer.create(eventloop, privKey.computeKeys(), sessionStore);
	}


	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new ContainerModule<SimpleUserContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new AuthModule<SimpleUserContainer>(SESSION_ID) {},
				override(new GlobalNodesModule(),
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

