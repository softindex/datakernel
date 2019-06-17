package io.datakernel.test;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public final class UnsupportedLoggingConfig {
	private static final String ROOT_LOGGER = "";

	public UnsupportedLoggingConfig() {
		Logger rootLogger = Logger.getLogger(ROOT_LOGGER);
		rootLogger.setLevel(Level.ALL);
		for (Handler handler : rootLogger.getHandlers()) {
			rootLogger.removeHandler(handler);
		}
		rootLogger.addHandler(new Handler() {
			@Override
			public void publish(LogRecord record) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void flush() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void close() throws SecurityException {
				throw new UnsupportedOperationException();
			}
		});
	}
}
