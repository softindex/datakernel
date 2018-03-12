package io.datakernel.http.boot;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggersModule;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Primary;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static com.google.inject.util.Modules.combine;
import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigInitializers.*;
import static io.datakernel.http.boot.ConfigInitializers.ofHttpServerTriggers;
import static io.datakernel.http.boot.ConfigInitializers.ofHttpWorker;
import static io.datakernel.jmx.JmxInitializers.ofGlobalEventloopStats;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class MultithreadedHttpServerLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "http-server.properties";
	public static final String PROPERTIES_FILE_EFFECTIVE = "http-server.effective.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	PrimaryServer primaryServer;

	@Override
	protected final Collection<Module> getModules() {
		return asList(
				override(getBaseModules()).with(getOverrideModules()),
				combine(getBusinessLogicModules()));
	}

	private Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create()
						.initialize(ofGlobalEventloopStats()),
				TriggersModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.saveEffectiveConfigTo(PROPERTIES_FILE_EFFECTIVE),
				new SimpleModule() {
					@Provides
					@Singleton
					@Primary
					public Eventloop provide(Config config,
					                         TriggerRegistry triggerRegistry) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.primary")))
								.initialize(ofEventloopTriggers(triggerRegistry, config.getChild("triggers.eventloop")));
					}

					@Provides
					@Worker
					public Eventloop provide(Config config,
					                         OptionalDependency<ThrottlingController> maybeThrottlingController,
					                         TriggerRegistry triggerRegistry) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.worker")))
								.initialize(ofEventloopTriggers(triggerRegistry, config.getChild("triggers.eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withThrottlingController));
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
					public AsyncHttpServer provideWorker(Eventloop eventloop, AsyncServlet rootServlet, TriggerRegistry triggerRegistry, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(ofHttpWorker(config.getChild("http")))
								.initialize(ofHttpServerTriggers(triggerRegistry, config.getChild("triggers.http")));
					}
				});
	}

	protected Collection<Module> getOverrideModules() {
		return emptyList();
	}

	protected abstract Collection<Module> getBusinessLogicModules();

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		String businessLogicModuleName = System.getProperty(BUSINESS_MODULE_PROP);
		Module businessLogicModule = businessLogicModuleName != null ?
				(Module) Class.forName(businessLogicModuleName).newInstance() :
				HelloWorldWorkerServletModule.create();

		Launcher launcher = new MultithreadedHttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(businessLogicModule);
			}
		};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
