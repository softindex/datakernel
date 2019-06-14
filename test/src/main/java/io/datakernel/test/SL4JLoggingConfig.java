package io.datakernel.test;

import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class SL4JLoggingConfig {
	private static final String ROOT_LOGGER = "";

	public SL4JLoggingConfig() {
		// The Root logger is not available on this stage
		// so we need to trigger its creation with getLogger call
		Logger.getLogger(ROOT_LOGGER).setLevel(Level.ALL);
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}
}
