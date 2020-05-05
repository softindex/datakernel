package io.datakernel.jmx.api;

public final class ConcurrentJmxBeanAdapter implements JmxBeanAdapter {
	@Override
	public void execute(Object bean, Runnable command) {
		command.run();
	}
}
