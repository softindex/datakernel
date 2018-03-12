package io.datakernel.config;

import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggerResult;
import io.datakernel.util.Initializer;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.Eventloop.DEFAULT_IDLE_INTERVAL;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.trigger.Severity.HIGH;
import static io.datakernel.trigger.Severity.WARNING;

public class ConfigInitializers {
	private ConfigInitializers() {
	}

	public static <T extends AbstractServer<T>> Initializer<T> ofAbstractServer(Config config) {
		return server -> server
				.withListenAddresses(config.get(ConfigConverters.ofList(ConfigConverters.ofInetSocketAddress()), "listenAddresses"))
				.withAcceptOnce(config.get(ConfigConverters.ofBoolean(), "acceptOnce", false))
				.withSocketSettings(config.get(ConfigConverters.ofSocketSettings(), "socketSettings", server.getSocketSettings()))
				.withServerSocketSettings(config.get(ConfigConverters.ofServerSocketSettings(), "serverSocketSettings", server.getServerSocketSettings()));

	}

	public static Initializer<PrimaryServer> ofPrimaryServer(Config config) {
		return ofAbstractServer(config);
	}

	public static Initializer<Eventloop> ofEventloop(Config config) {
		return eventloop -> eventloop
				.withFatalErrorHandler(config.get(ofFatalErrorHandler(), "fatalErrorHandler", rethrowOnAnyError()))
				.withIdleInterval(config.get(ofLong(), "idleIntervalMillis", DEFAULT_IDLE_INTERVAL))
				.withThreadPriority(config.get(ofInteger(), "threadPriority", 0));
	}

	public static Initializer<Eventloop> ofEventloopTriggers(TriggerRegistry triggersRegistry, Config config) {
		return eventloop -> {
			int businessLogicTimeWarning = config.get(ofInteger(), "businessLogicTime.warning", 10);
			int businessLogicTimeHigh = config.get(ofInteger(), "businessLogicTime.high", 100);
			triggersRegistry.add(HIGH, "fatalErrors", () ->
					TriggerResult.ofError(eventloop.getStats().getFatalErrors()));
			triggersRegistry.add(WARNING, "businessLogic", () ->
					TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
							businessLogicTime -> businessLogicTime > businessLogicTimeWarning));
			triggersRegistry.add(HIGH, "businessLogic", () ->
					TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
							businessLogicTime -> businessLogicTime > businessLogicTimeHigh));
		};
	}
}
