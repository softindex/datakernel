package io.global.todo;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.common.PrivKey;
import io.global.kv.GlobalKvDriver;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.MapModule;
import io.global.ot.OTAppCommonModule;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.SimpleUserContainer;
import io.global.ot.session.AuthModule;
import io.global.ot.session.UserId;
import io.global.session.KvSessionStore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.OTUtils.REGISTRY;

public final class GlobalTodoApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-todo.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Todo List";
	private static final String TODO_LIST_REPO = "todo/list";
	private static final String TODO_LIST_SESSION_TABLE = "todo/session";
	private static final String SESSION_ID = "TODO_SID";
	private static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");

	@Inject
	AsyncHttpServer server;

	@Inject
	GlobalKvDriver<String, UserId> kvDriver;

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
				.with("node.serverId", DEFAULT_SERVER_ID)
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTUplinkServlet<MapOperation<String, Boolean>> todoListServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/list/*", sessionDecorator.serve(todoListServlet))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
	}

	@Provides
	CodecFactory codecFactory() {
		return REGISTRY;
	}

	@Provides
	@ContainerScope
	SimpleUserContainer userContainer(Eventloop eventloop, PrivKey privKey, GlobalKvDriver<String, UserId> kvDriver) {
		KvSessionStore<UserId> sessionStore = KvSessionStore.create(eventloop, kvDriver.adapt(privKey), TODO_LIST_SESSION_TABLE);
		return SimpleUserContainer.create(eventloop, privKey.computeKeys(), sessionStore);
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new OTAppCommonModule(),
				new AuthModule<SimpleUserContainer>(SESSION_ID) {},
				new ContainerModule<SimpleUserContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new MapModule<String, Boolean>(TODO_LIST_REPO) {},
				// override for debug purposes
				override(new GlobalNodesModule(),
						new LocalNodeCommonModule(DEFAULT_SERVER_ID))
		);
	}

	@Override
	public void run() throws Exception {
		logger.info("Application running on http://localhost:8080/");
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalTodoApp().launch(args);
	}
}
