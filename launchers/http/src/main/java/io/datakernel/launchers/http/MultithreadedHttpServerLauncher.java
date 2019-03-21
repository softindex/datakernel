package io.datakernel.launchers.http;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.worker.Primary;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import io.datakernel.worker.WorkerPool;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static com.google.inject.util.Modules.combine;
import static com.google.inject.util.Modules.override;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.jmx.JmxModuleInitializers.ofGlobalEventloopStats;
import static io.datakernel.launchers.initializers.Initializers.*;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class MultithreadedHttpServerLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "http-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	PrimaryServer primaryServer;

	@Override
	protected final Collection<com.google.inject.Module> getModules() {
		return asList(
				override(getBaseModules()).with(getOverrideModules()),
				combine(getBusinessLogicModules()));
	}

	private Collection<com.google.inject.Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create()
						.initialize(ofGlobalEventloopStats()),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					@Singleton
					@Primary
					public Eventloop provideEventloop(Config config) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.primary")));
					}

					@Provides
					@Worker
					public Eventloop provide(Config config, OptionalDependency<ThrottlingController> maybeThrottlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.worker")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withInspector));
					}

					@Provides
					@Singleton
					public WorkerPool provide(Config config) {
						return new WorkerPool(config.get(ofInteger(), "workers", 4));
					}

					@Provides
					@Singleton
					public PrimaryServer providePrimaryServer(@Primary Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
						List<AsyncHttpServer> workerHttpServers = workerPool.getInstances(AsyncHttpServer.class);
						return PrimaryServer.create(primaryEventloop, workerHttpServers)
								.initialize(ofPrimaryServer(config.getChild("http")));
					}

					@Provides
					@Worker
					public AsyncHttpServer provideWorker(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(ofHttpWorker(config.getChild("http")));
					}
				});
	}

	protected Collection<com.google.inject.Module> getOverrideModules() {
		return emptyList();
	}

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
					@Worker
					AsyncServlet provideServlet(@WorkerId int worker) {
						return req -> Promise.of(
								HttpResponse.ok200().withBody(ByteBuf.wrapForReading(encodeAscii("Hello, world! #" + worker))));
					}
				};

		Launcher launcher = new MultithreadedHttpServerLauncher() {
			@Override
			protected Collection<com.google.inject.Module> getBusinessLogicModules() {
				return singletonList(businessLogicModule);
			}
		};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
