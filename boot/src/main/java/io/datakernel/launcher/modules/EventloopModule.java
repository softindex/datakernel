package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigUtils;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.guice.SimpleModule;

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
	public Eventloop provide(Config config) {
		return Eventloop.create()
				.initialize(eventloop -> ConfigUtils.initializeEventloop(eventloop, config.getChild("eventloop")));
	}
}
