package io.datakernel.jmx.api;

public interface JmxWrapperFactory {
	MBeanWrapper wrap(Object instance);
}
