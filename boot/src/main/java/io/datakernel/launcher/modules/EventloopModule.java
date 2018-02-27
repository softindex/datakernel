package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.guice.SimpleModule;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.Eventloop.DEFAULT_IDLE_INTERVAL;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

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
		Long idleInterval = config.get(ofLong(), "eventloop.idleIntervalMillis", DEFAULT_IDLE_INTERVAL);
		Integer threadPriority = config.get(ofInteger(), "eventloop.threadPriority", 0);

		return Eventloop.create()
				.withFatalErrorHandler(config.get(ofFatalErrorHandler(), "eventloop.fatalErrorHandler", rethrowOnAnyError()))
				.withThrottlingController(config.getOrNull(ofThrottlingController(), "eventloop.throttlingController"))
				.withIdleInterval(idleInterval)
				.withThreadPriority(threadPriority)
				.withCurrentThread();
	}
}
