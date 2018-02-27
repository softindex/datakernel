package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.Eventloop.DEFAULT_IDLE_INTERVAL;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * This module provides an unnamed worker {@link Eventloop eventloop} instance.
 */
public class WorkerEventloopModule extends SimpleModule {

	// region creators
	private WorkerEventloopModule() {
	}

	public static WorkerEventloopModule create() {
		return new WorkerEventloopModule();
	}
	// endregion

	@Provides
	@Worker
	public Eventloop provide(Config config, @WorkerId int workerId) {

		Config conf = config.getChild("eventloop.workers")
				.override(config.getChild("eventloop.worker" + workerId));

		Long idleInterval = conf.get(ofLong(), "idleIntervalMillis", DEFAULT_IDLE_INTERVAL);
		Integer threadPriority = conf.get(ofInteger(), "threadPriority", 0);

		return Eventloop.create()
				.withFatalErrorHandler(conf.get(ofFatalErrorHandler(), "fatalErrorHandler", rethrowOnAnyError()))
				.withThrottlingController(conf.getOrNull(ofThrottlingController(), "throttlingController"))
				.withIdleInterval(idleInterval)
				.withThreadPriority(threadPriority)
				.withCurrentThread();
	}
}
