package io.datakernel.jmx.api;

public interface JmxWrapperFactory {
	void execute(Object instance, Runnable command);
}
