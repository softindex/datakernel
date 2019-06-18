package io.datakernel.test;

import java.util.logging.ConsoleHandler;

public final class StdoutConsoleHandler extends ConsoleHandler {
	public StdoutConsoleHandler() {
		setOutputStream(System.out);
	}
}
