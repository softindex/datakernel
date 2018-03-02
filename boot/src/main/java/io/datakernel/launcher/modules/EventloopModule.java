package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.guice.SimpleModule;

import static io.datakernel.config.ConfigUtils.initializeEventloop;

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
	public Eventloop provide(Config config, ThrottlingControllerInitializer throttlingControllerInitializer) {
		return Eventloop.create()
				.initialize(eventloop -> initializeEventloop(eventloop, config.getChild("eventloop")))
				.initialize(throttlingControllerInitializer);
	}
}
