package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.SimpleModule;

import static io.datakernel.config.ConfigUtils.initializeEventloop;
import static io.datakernel.config.ConfigUtils.initializeEventloopTriggers;

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
	                         OptionalDependency<ThrottlingController> optionalThrottlingController,
	                         TriggerRegistry triggerRegistry) {
		return Eventloop.create()
				.initialize(eventloop -> initializeEventloop(eventloop, config.getChild("eventloop")))
				.initialize(eventloop -> optionalThrottlingController.ifPresent(eventloop::withThrottlingController))
				.initialize(eventloop -> initializeEventloopTriggers(eventloop, triggerRegistry, config.getChild("triggers.eventloop")));
	}

}
