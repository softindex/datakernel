package io.datakernel.jmx.api;

public final class ConcurrentJmxMBeanFactory implements JmxWrapperFactory {
	@Override
	public void execute(Object instance, Runnable command) {
		command.run();
	}
}
