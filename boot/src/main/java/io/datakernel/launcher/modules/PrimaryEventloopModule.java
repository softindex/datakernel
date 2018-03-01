package io.datakernel.launcher.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.guice.SimpleModule;
import io.datakernel.worker.Primary;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.Eventloop.DEFAULT_IDLE_INTERVAL;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

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
	public Eventloop provide(Config config) {
		Long idleInterval = config.get(ofLong(), "eventloop.primary.idleIntervalMillis", DEFAULT_IDLE_INTERVAL);
		Integer threadPriority = config.get(ofInteger(), "eventloop.primary.threadPriority", 0);

		return Eventloop.create()
				.withFatalErrorHandler(config.get(ofFatalErrorHandler(), "eventloop.primary.fatalErrorHandler", rethrowOnAnyError()))
				.withThrottlingController(config.getOrNull(ofThrottlingController(), "eventloop.primary.throttlingController"))
				.withIdleInterval(idleInterval)
				.withThreadPriority(threadPriority);
	}
}
