package io.datakernel.launchers.http;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofSystemProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.*;

/**
 * Preconfigured Http server launcher.
 *
 * @see Launcher
 */
public abstract class HttpServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "http-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	AsyncHttpServer httpServer;

	@Provides
	Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	AsyncHttpServer server(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
		return AsyncHttpServer.create(eventloop, rootServlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create()
						.initialize(ofAsyncComponents()),
				JmxModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
				getBusinessLogicModule()
		);
	}

	/**
	 * Override this method to supply your launcher business logic.
	 */
	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is now available at " + String.join(", ", httpServer.getHttpAddresses()));
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		String businessLogicModuleName = System.getProperty(BUSINESS_MODULE_PROP);

		Module businessLogicModule = businessLogicModuleName != null ?
				(Module) Class.forName(businessLogicModuleName).newInstance() :
				new AbstractModule() {
					@Provides
					public AsyncServlet servlet(Config config) {
						String message = config.get("message", "Hello, world!");
						return request -> HttpResponse.ok200().withPlainText(message);
					}
				};

		Launcher launcher = new HttpServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return businessLogicModule;
			}
		};

		launcher.launch(args);
	}
}
