package io.datakernel.launchers.http;

import io.datakernel.async.Promise;
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
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPools;

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBuf.wrapForReading;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.jmx.JmxModuleInitializers.ofGlobalEventloopStats;
import static io.datakernel.launchers.initializers.Initializers.*;

public abstract class MultithreadedHttpServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "http-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	PrimaryServer primaryServer;

	@Override
	protected final Module getModule() {
		return combine(
				override(getBaseModule(), getOverrideModule()),
				getBusinessLogicModule());
	}

	private Module getBaseModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create()
						.initialize(ofGlobalEventloopStats()),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
								.override(ofClassPathProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					Eventloop primaryEventloop(Config config) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.primary")));
					}

					@Provides
					@Worker
					Eventloop workerEventloop(Config config, @Optional ThrottlingController throttlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop.worker")))
								.initialize(eventloop -> eventloop.withInspector(throttlingController));
					}

					@Provides
					WorkerPool workerPool(WorkerPools workerPools, Config config) {
						return workerPools.createPool(config.get(ofInteger(), "workers", 4));
					}

					@Provides
					PrimaryServer primaryServer(Eventloop primaryEventloop, WorkerPool.Instances<AsyncHttpServer> workerServers, Config config) {
						return PrimaryServer.create(primaryEventloop, workerServers.getList())
								.initialize(ofPrimaryServer(config.getChild("http")));
					}

					@Provides
					WorkerPool.Instances<AsyncHttpServer> workerServers(WorkerPool workerPool) {
						return workerPool.getInstances(AsyncHttpServer.class);
					}

					@Provides
					@Worker
					AsyncHttpServer workerServer(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
						return AsyncHttpServer.create(eventloop, rootServlet)
								.initialize(ofHttpWorker(config.getChild("http")));
					}
				});
	}

	protected Module getOverrideModule() {
		return Module.empty();
	}

	protected abstract Module getBusinessLogicModule();

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
					AsyncServlet servlet(@WorkerId int workerId) {
						return req -> Promise.of(HttpResponse.ok200().withBody(wrapForReading(encodeAscii("Hello, world! #" + workerId))));
					}
				};

		Launcher launcher = new MultithreadedHttpServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return businessLogicModule;
			}
		};

		launcher.launch(args);
	}
}
