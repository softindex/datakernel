package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.SimpleModule;

import static io.datakernel.config.Initializers.ofEventloop;
import static io.datakernel.config.Initializers.ofEventloopTriggers;

/**
 * This module provides an unnamed singleton {@link Eventloop eventloop} instance.
 */
public class EventloopModule extends SimpleModule {

	// region creators
	private EventloopModule() {
	}

	public static EventloopModule create() {
		return new EventloopModule();
	}
	// endregion

	@Provides
	@Singleton
	public Eventloop provide(Config config,
	                         OptionalDependency<ThrottlingController> maybeThrottlingController,
	                         TriggerRegistry triggerRegistry) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(ofEventloopTriggers(triggerRegistry, config.getChild("triggers.eventloop")))
				.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withThrottlingController));
	}

}
