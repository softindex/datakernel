package io.datakernel.test.rules;

import java.util.logging.Level;

public enum LoggerLevel {
	OFF(Level.OFF), SEVERE(Level.SEVERE), WARNING(Level.WARNING), INFO(Level.INFO), CONFIG(Level.CONFIG), FINE(Level.FINE), FINEST(Level.FINEST), ALL(Level.ALL);

	private final Level level;

	LoggerLevel(Level level) {
		this.level = level;
	}

	public Level getLevel() {
		return level;
	}
}
