package io.global.todo;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.MapModule;
import io.global.ot.OTAppCommonModule;
import io.global.ot.map.MapOperation;

import java.util.Comparator;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;

public final class GlobalTodoApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-todo.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Todo List";
	private static final String TODO_LIST_REPO = "todo/list";

	@Inject
	AsyncHttpServer server;

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
				.with("node.serverId", DEFAULT_SERVER_ID)
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	AsyncServlet mainServlet(
			DynamicOTNodeServlet<MapOperation<String, Boolean>> todoListServlet,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/list/*", todoListServlet)
				.map("/*", staticServlet);
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new OTAppCommonModule() {
					@Override
					protected void configure() {
						bind(CodecFactory.class).toInstance(REGISTRY);
						bind(new Key<Comparator<Boolean>>() {}).toInstance(Boolean::compareTo);
					}
				},
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
