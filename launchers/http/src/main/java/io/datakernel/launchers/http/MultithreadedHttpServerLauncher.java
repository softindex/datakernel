package io.datakernel.launchers.http;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.net.PrimaryServer;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.*;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import static io.datakernel.bytebuf.ByteBuf.wrapForReading;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.jmx.JmxModuleInitializers.ofGlobalEventloopStats;
import static io.datakernel.launchers.initializers.Initializers.*;
import static java.util.stream.Collectors.joining;

@SuppressWarnings("WeakerAccess")
public abstract class MultithreadedHttpServerLauncher extends Launcher {
	public static final int PORT = 8080;
	public static final int WORKERS = 4;

	public static final String PROPERTIES_FILE = "http-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	PrimaryServer primaryServer;

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
	@Worker
	AsyncHttpServer workerServer(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpWorker(config.getChild("http")));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT)))
				.with("workers", "" + WORKERS)
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				WorkerPoolModule.create(),
				JmxModule.create()
						.initialize(ofGlobalEventloopStats()),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				getBusinessLogicModule()
		);
	}

	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	@Override
	protected void run() throws Exception {
		logger.info("HTTP Server is listening on " + Stream.concat(
				primaryServer.getListenAddresses().stream().map(address -> "http://" + ("0.0.0.0".equals(address.getHostName()) ? "localhost" : address.getHostName()) + (address.getPort() != 80 ? ":" + address.getPort() : "") + "/"),
				primaryServer.getSslListenAddresses().stream().map(address -> "https://" + ("0.0.0.0".equals(address.getHostName()) ? "localhost" : address.getHostName()) + (address.getPort() != 80 ? ":" + address.getPort() : "") + "/"))
				.collect(joining(" ")));
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
