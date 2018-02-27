package io.datakernel.http.boot;

import com.google.inject.Provides;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Worker;

import java.util.Collections;

import static io.datakernel.http.boot.HttpServerModule.getHttpServerInitializer;

public class WorkerHttpServerModule extends SimpleModule {

	// region creators
	private WorkerHttpServerModule() {
	}

	public static WorkerHttpServerModule create() {
		return new WorkerHttpServerModule();
	}
	// endregion

	@Provides
	@Worker
	public AsyncHttpServer provide(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
		return AsyncHttpServer.create(eventloop, rootServlet)
				.initialize(getHttpServerInitializer(config.getChild("http.worker")))
				.withListenAddresses(Collections.emptyList()); // remove any listen adresses
	}
}
