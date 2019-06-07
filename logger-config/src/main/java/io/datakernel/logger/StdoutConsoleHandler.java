package io.datakernel.logger;

import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

public final class StdoutConsoleHandler extends StreamHandler {
	public StdoutConsoleHandler() {
		super();
		setOutputStream(System.out);
	}

	@Override
	public void publish(LogRecord record) {
		super.publish(record);
		flush();
	}

	@Override
	public void close() {
		flush();
	}
}
