package io.datakernel.http.boot;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigsModule;
import io.datakernel.config.impl.PropertiesConfig;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.modules.PrimaryEventloopModule;
import io.datakernel.launcher.modules.WorkerEventloopModule;
import io.datakernel.launcher.modules.WorkerPoolModule;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Primary;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.inject.util.Modules.combine;
import static com.google.inject.util.Modules.override;
import static io.datakernel.config.ConfigConverters.ofAbstractServerInitializer;
import static io.datakernel.http.boot.HttpConfigUtils.getHttpServerInitializer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class MultithreadedHttpServerLauncher extends Launcher {
	public static final String PRODUCTION_MODE = "production";
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

	protected final Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigsModule.create(PropertiesConfig.ofProperties(PROPERTIES_FILE, true))
						.saveEffectiveConfigTo(PROPERTIES_FILE_EFFECTIVE),
				WorkerPoolModule.create(),
				PrimaryEventloopModule.create(),
				WorkerEventloopModule.create(),
				new SimpleModule(){
					@Provides
					@Singleton
					public PrimaryServer providePrimaryServer(@Primary Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
						List<AsyncHttpServer> workerHttpServers = workerPool.getInstances(AsyncHttpServer.class);
						return PrimaryServer.create(primaryEventloop, workerHttpServers)
								.initialize(config.get(ofAbstractServerInitializer(new InetSocketAddress(8080)), "http.primary.server"));
					}

					@Provides
					@Worker
					public AsyncHttpServer provide(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(getHttpServerInitializer(config.getChild("http.worker"), new InetSocketAddress(8080)))
								.withListenAddresses(Collections.emptyList()); // remove any listen adresses
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
		launcher.launch(parseBoolean(System.getProperty(PRODUCTION_MODE)), args);
	}
}
