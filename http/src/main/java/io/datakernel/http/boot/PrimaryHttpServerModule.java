package io.datakernel.http.boot;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.worker.Primary;
import io.datakernel.worker.WorkerPool;

import java.util.List;

import static io.datakernel.config.ConfigConverters.ofAbstractServerInitializer;

public class PrimaryHttpServerModule extends AbstractModule {

	// region creators
	private PrimaryHttpServerModule() {
	}

	public static PrimaryHttpServerModule create() {
		return new PrimaryHttpServerModule();
	}
	// endregion

	@Override
	protected void configure() {
	}

	@Provides
	@Singleton
	public PrimaryServer providePrimaryServer(@Primary Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
		List<AsyncHttpServer> workerHttpServers = workerPool.getInstances(AsyncHttpServer.class);
		return PrimaryServer.create(primaryEventloop, workerHttpServers)
				.initialize(config.get(ofAbstractServerInitializer(8080), "http.primary.server"));
	}
}
