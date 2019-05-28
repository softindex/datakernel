package io.datakernel.launchers.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.Inject;
import io.datakernel.di.Optional;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.*;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.jmx.JmxModuleInitializers.ofGlobalEventloopStats;
import static io.datakernel.launchers.initializers.Initializers.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class MultithreadedHttpServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "http-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	PrimaryServer primaryServer;

	@Override
	protected final Collection<Module> getModules() {
		return asList(
				override(getBaseModules(), getOverrideModules()),
				combine(getBusinessLogicModules()));
	}

	private Collection<Module> getBaseModules() {
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
				new WorkerPoolModule(),
				new AbstractModule() {
					@Provides
					@Primary
					Eventloop provideEventloop(Config config) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.primary")));
					}

					@Provides
					@Worker
					Eventloop provide(Config config, @Optional ThrottlingController throttlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.worker")))
								.initialize(eventloop -> eventloop.withInspector(throttlingController));
					}

					@Provides
					WorkerPool workerPool(WorkerPools workerPools, Config config) {
						return workerPools.createPool(config.get(ofInteger(), "workers", 4));
					}

					@Provides
					PrimaryServer providePrimaryServer(@Primary Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
						List<AsyncHttpServer> workerHttpServers = workerPool.getInstances(AsyncHttpServer.class);
						return PrimaryServer.create(primaryEventloop, workerHttpServers)
								.initialize(ofPrimaryServer(config.getChild("http")));
					}

					@Provides
					@Worker
					AsyncHttpServer provideWorker(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(ofHttpWorker(config.getChild("http")));
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
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(businessLogicModule);
			}
		};
		launcher.launch(args);
	}
}
