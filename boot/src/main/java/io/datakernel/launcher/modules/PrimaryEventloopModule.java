package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Primary;

import static io.datakernel.config.Initializers.ofEventloop;
import static io.datakernel.config.Initializers.ofEventloopTriggers;

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
	public Eventloop provide(Config config,
	                         TriggerRegistry triggerRegistry) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop.primary")))
				.initialize(ofEventloopTriggers(triggerRegistry, config.getChild("triggers.eventloop")));
	}
}
