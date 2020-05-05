package io.datakernel.jmx.helper;

import io.datakernel.jmx.api.JmxWrapperFactory;

public final class StubWrapperFactory implements JmxWrapperFactory {
	@Override
	public void execute(Object instance, Runnable command) {
		command.run();
	}
}
