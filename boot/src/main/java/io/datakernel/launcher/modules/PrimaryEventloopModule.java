package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Primary;

import static io.datakernel.config.ConfigUtils.initializeEventloop;

/**
 * This module provides a singleton primary {@link Eventloop eventloop} instance.
 */
public class PrimaryEventloopModule extends SimpleModule {

	// region creators
	private PrimaryEventloopModule() {
	}

	public static PrimaryEventloopModule create() {
		return new PrimaryEventloopModule();
	}
	// endregion

	@Provides
	@Primary
	@Singleton
	public Eventloop provide(Config config, ThrottlingControllerInitializer throttlingControllerInitializer) {
		return Eventloop.create()
				.initialize(eventloop -> initializeEventloop(eventloop, config.getChild("eventloop.primary")))
				.initialize(throttlingControllerInitializer);
	}
}
