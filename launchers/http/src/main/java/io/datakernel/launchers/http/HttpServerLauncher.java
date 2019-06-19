package io.datakernel.launchers.http;

import io.datakernel.async.Promise;
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
import java.util.stream.Stream;

import static io.datakernel.bytebuf.ByteBuf.wrapForReading;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static java.util.stream.Collectors.joining;

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

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
								.override(ofClassPathProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				getBusinessLogicModule());
	}

	/**
	 * Override this method to supply your launcher business logic.
	 */
	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is listening on " + Stream.concat(
				httpServer.getListenAddresses().stream().map(address -> "http://" + ("0.0.0.0".equals(address.getHostName()) ? "localhost" : address.getHostName()) + (address.getPort() != 80 ? ":" + address.getPort() : "") + "/"),
				httpServer.getSslListenAddresses().stream().map(address -> "https://" + ("0.0.0.0".equals(address.getHostName()) ? "localhost" : address.getHostName()) + (address.getPort() != 80 ? ":" + address.getPort() : "") + "/"))
				.collect(joining(" ")));
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
						return req -> Promise.of(
								HttpResponse.ok200().withBody(wrapForReading(encodeAscii(message))));
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
