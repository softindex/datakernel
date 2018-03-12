package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Worker;

import static io.datakernel.config.Initializers.ofEventloop;
import static io.datakernel.config.Initializers.ofEventloopTriggers;

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
	public Eventloop provide(Config config,
	                         OptionalDependency<ThrottlingController> maybeThrottlingController,
	                         TriggerRegistry triggerRegistry) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop.worker")))
				.initialize(ofEventloopTriggers(triggerRegistry, config.getChild("triggers.eventloop")))
				.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withThrottlingController));
	}
}
