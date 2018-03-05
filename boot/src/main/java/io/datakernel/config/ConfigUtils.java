package io.datakernel.config;

import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggerResult;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.Eventloop.DEFAULT_IDLE_INTERVAL;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.trigger.Severity.HIGH;
import static io.datakernel.trigger.Severity.WARNING;

public class ConfigUtils {
	private ConfigUtils() {
	}

	public static <T extends AbstractServer<T>> void initializeAbstractServer(T server, Config config) {
		server.withListenAddresses(config.get(ConfigConverters.ofList(ConfigConverters.ofInetSocketAddress()), "listenAddresses"));
		server.withAcceptOnce(config.get(ConfigConverters.ofBoolean(), "acceptOnce", false));
		server.withSocketSettings(config.get(ConfigConverters.ofSocketSettings(), "socketSettings", server.getSocketSettings()));
		server.withServerSocketSettings(config.get(ConfigConverters.ofServerSocketSettings(), "serverSocketSettings", server.getServerSocketSettings()));
	}

	public static void initializePrimaryServer(PrimaryServer server, Config config) {
		initializeAbstractServer(server, config);
	}

	public static void initializeEventloop(Eventloop eventloop, Config config) {
		eventloop.withFatalErrorHandler(config.get(ofFatalErrorHandler(), "fatalErrorHandler", rethrowOnAnyError()));
		eventloop.withIdleInterval(config.get(ofLong(), "idleIntervalMillis", DEFAULT_IDLE_INTERVAL));
		eventloop.withThreadPriority(config.get(ofInteger(), "threadPriority", 0));
	}

	public static void initializeEventloopTriggers(Eventloop eventloop, TriggerRegistry triggersRegistry, Config config) {
		int businessLogicTimeWarning = config.get(ConfigConverters.ofInteger(), "businessLogicTime.warning", 10);
		int businessLogicTimeHigh = config.get(ConfigConverters.ofInteger(), "businessLogicTime.high", 100);
		triggersRegistry.add(HIGH, "fatalErrors", () ->
				TriggerResult.ofError(eventloop.getStats().getFatalErrors()));
		triggersRegistry.add(WARNING, "businessLogic", () ->
				TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
						businessLogicTime -> businessLogicTime > businessLogicTimeWarning));
		triggersRegistry.add(HIGH, "businessLogic", () ->
				TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
						businessLogicTime -> businessLogicTime > businessLogicTimeHigh));
	}

}
