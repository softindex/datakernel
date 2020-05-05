package io.datakernel.jmx.api;

public interface JmxBeanAdapter {
	void execute(Object bean, Runnable command);
}
