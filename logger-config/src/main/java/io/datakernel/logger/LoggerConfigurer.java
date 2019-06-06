package io.datakernel.logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static java.lang.ClassLoader.getSystemClassLoader;

public final class LoggerConfigurer {
	public static final String ROOT_LOGGER = "";
	public static final Level DEFAULT_LEVEL = Level.ALL;
	static {
		try {
			LogManager.getLogManager().readConfiguration(getSystemClassLoader().getResourceAsStream("logger.properties"));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static void enableLogging() {
		enableLogging(ROOT_LOGGER, DEFAULT_LEVEL);
	}

	public static void enableLogging(Level level) {
		enableLogging(ROOT_LOGGER, level);
	}

	public static void enableLogging(Class<?> cls) {
		enableLogging(cls, DEFAULT_LEVEL);
	}

	public static void enableLogging(String name) {
		enableLogging(name, DEFAULT_LEVEL);
	}

	public static void enableLogging(Class<?> cls, Level level) {
		enableLogging(cls.getName(), level);
	}

	public static void enableLogging(String name, Level level) {
		Logger rootLogger = Logger.getLogger(name);
		rootLogger.setLevel(level);
	}
}
