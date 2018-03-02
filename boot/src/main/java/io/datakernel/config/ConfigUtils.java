package io.datakernel.config;

import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.eventloop.Eventloop.DEFAULT_IDLE_INTERVAL;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

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
		eventloop.withThrottlingController(config.getOrNull(ofThrottlingController(), "throttlingController"));
		eventloop.withIdleInterval(config.get(ofLong(), "idleIntervalMillis", DEFAULT_IDLE_INTERVAL));
		eventloop.withThreadPriority(config.get(ofInteger(), "threadPriority", 0));
	}
}
