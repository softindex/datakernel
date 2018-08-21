package io.datakernel.launchers.http;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;

import java.net.InetSocketAddress;
import java.util.Collection;

import static com.google.inject.util.Modules.combine;
import static com.google.inject.util.Modules.override;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.http.HttpResponse.ok200;
import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.Initializers.ofHttpServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Preconfigured Http server launcher.
 *
 * @see Launcher
 */
public abstract class HttpServerLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "http-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	AsyncHttpServer httpServer;

	@Override
	protected final Collection<com.google.inject.Module> getModules() {
		return asList(
				override(getBaseModules()).with(getOverrideModules()),
				combine(getBusinessLogicModules()));
	}

	private Collection<com.google.inject.Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					@Singleton
					Eventloop provide(Config config,
							OptionalDependency<ThrottlingController> maybeThrottlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withThrottlingController));
					}

					@Provides
					@Singleton
					AsyncHttpServer provide(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(ofHttpServer(config.getChild("http")));
					}
				}
		);
	}

	/**
	 * Override this method to override base modules supplied in launcher.
	 */
	protected Collection<com.google.inject.Module> getOverrideModules() {
		return emptyList();
	}

	/**
	 * Override this method to supply your launcher business logic.
	 */
	protected abstract Collection<com.google.inject.Module> getBusinessLogicModules();

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		String businessLogicModuleName = System.getProperty(BUSINESS_MODULE_PROP);
		com.google.inject.Module businessLogicModule = businessLogicModuleName != null ?
				(com.google.inject.Module) Class.forName(businessLogicModuleName).newInstance() :
				new AbstractModule() {
					@Provides
					public AsyncServlet provide(Config config) {
						String message = config.get("message", "Hello, world!");
						return AsyncServlet.ofBlocking(req -> ok200()
								.withBody(ByteBuf.wrapForReading(encodeAscii(message))));
					}
				};

		Launcher launcher = new HttpServerLauncher() {
			@Override
			protected Collection<com.google.inject.Module> getBusinessLogicModules() {
				return singletonList(businessLogicModule);
			}
		};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
