package io.datakernel.logger.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.logging.LogManager;

import static java.lang.ClassLoader.getSystemClassLoader;

public final class SimpleLoggingConfig {
	public SimpleLoggingConfig() {
		try {
			LogManager.getLogManager().readConfiguration(getSystemClassLoader()
					.getResourceAsStream("logger.properties"));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}

