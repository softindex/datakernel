package io.datakernel.jmx.helper;

import io.datakernel.jmx.api.JmxBeanAdapter;

public final class JmxBeanAdapterStub implements JmxBeanAdapter {
	@Override
	public void execute(Object bean, Runnable command) {
		command.run();
	}
}
