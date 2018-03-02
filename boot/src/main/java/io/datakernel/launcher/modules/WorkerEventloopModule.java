package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Worker;

import static io.datakernel.config.ConfigUtils.initializeEventloop;

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
	public Eventloop provide(Config config, ThrottlingControllerInitializer throttlingControllerInitializer) {
		return Eventloop.create()
				.initialize(eventloop -> initializeEventloop(eventloop, config.getChild("eventloop.worker")))
				.initialize(throttlingControllerInitializer);
	}
}
