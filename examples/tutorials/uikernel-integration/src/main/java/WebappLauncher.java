import com.google.gson.Gson;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.uikernel.UiKernelServlets;

import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofString;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.ofAsyncComponents;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class WebappLauncher extends Launcher {
	private static final int DEFAULT_PORT = 8080;
	private static final String DEFAULT_PATH_TO_RESOURCES = "/static";

	@Inject
	AsyncHttpServer server;

	@Provides
	Gson gson() {
		return new Gson();
	}

	@Provides
	Config config() {
		return Config.ofClassPathProperties("configs.properties");
	}

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	@Provides
	StaticLoader staticLoader(Executor executor, Config config) {
		return StaticLoader.ofClassPath(executor, config.get(ofString(), "resources", DEFAULT_PATH_TO_RESOURCES));
	}

	@Provides
	AsyncServlet servlet(StaticLoader staticLoader, Gson gson, PersonGridModel model, Config config) {
		StaticServlet staticServlet = StaticServlet.create(staticLoader)
				.withIndexHtml();
		AsyncServlet usersApiServlet = UiKernelServlets.apiServlet(model, gson);

		return RoutingServlet.create()
				.map("/*", staticServlet)              // serves request if no other servlet matches
				.map("/api/users/*", usersApiServlet); // our rest crud servlet that would serve the grid
	}

	@Provides
	AsyncHttpServer server(Eventloop eventloop, Config config, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.withListenPort(config.get(ofInteger(), "port", DEFAULT_PORT));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create()
						.initialize(ofAsyncComponents()),
				ConfigModule.create()
						.withEffectiveConfigLogger());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		WebappLauncher launcher = new WebappLauncher();
		launcher.launch(args);
	}
}
