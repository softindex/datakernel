package io.datakernel.logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static java.lang.ClassLoader.getSystemResourceAsStream;

public final class LoggerFactory {
	static {
		try {
			LogManager.getLogManager()
					.readConfiguration(getSystemResourceAsStream("logger-config.properties"));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static Logger getGlobal() {
		return Logger.getGlobal();
	}

	public static Logger getLogger(Class<?> clazz) {
		return Logger.getLogger(clazz.getName());
	}

	public static Logger getLogger(String name) {
		return Logger.getLogger(name);
	}

	public static Logger getLogger(String name, String resourceBundleName) {
		return Logger.getLogger(name, resourceBundleName);
	}

	public static Logger getAnonymousLogger() {
		return Logger.getAnonymousLogger();
	}

	public static Logger getAnonymousLogger(String resourceBundleName) {
		return Logger.getAnonymousLogger();
	}
}
