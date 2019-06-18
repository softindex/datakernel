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
import io.datakernel.launcher.Launcher;
import io.datakernel.loader.StaticLoader;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.uikernel.UiKernelServlets;

import java.util.concurrent.ExecutorService;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofString;
import static io.datakernel.di.module.Modules.combine;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newCachedThreadPool;

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
		return Config.ofClassPathProperties(currentThread().getContextClassLoader(), "configs.properties");
	}

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	ExecutorService executorService() {
		return newCachedThreadPool();
	}

	@Provides
	StaticLoader staticLoader(Config config) {
		return StaticLoader.ofClassPath(currentThread().getContextClassLoader(),
				config.get(ofString(), "resources", DEFAULT_PATH_TO_RESOURCES));
	}

	@Provides
	AsyncServlet servlet(StaticLoader resourceLoader, Gson gson, PersonGridModel model, Config config) {
		StaticServlet staticServlet = StaticServlet.create(resourceLoader)
				.withIndexHtml();
		AsyncServlet usersApiServlet = UiKernelServlets.apiServlet(model, gson);

		return RoutingServlet.create()
				.with("/*", staticServlet)              // serves request if no other servlet matches
				.with("/api/users/*", usersApiServlet); // our rest crud servlet that would serve the grid
	}

	@Provides
	AsyncHttpServer server(Eventloop eventloop, Config config, AsyncServlet servlet) {
		return AsyncHttpServer.create(eventloop, servlet)
				.withListenPort(config.get(ofInteger(), "port", DEFAULT_PORT));
	}

	@Override
	protected Module getModule() {
		return combine(ServiceGraphModule.defaultInstance(), ConfigModule.create());
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
