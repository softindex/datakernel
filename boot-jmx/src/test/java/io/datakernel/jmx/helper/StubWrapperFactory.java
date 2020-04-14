package io.datakernel.jmx.helper;

import io.datakernel.jmx.api.JmxWrapperFactory;
import io.datakernel.jmx.api.MBeanWrapper;

public final class StubWrapperFactory implements JmxWrapperFactory {
	@Override
	public MBeanWrapper wrap(Object instance) {
		return new MBeanWrapper() {
			@Override
			public void execute(Runnable command) {
			}

			@Override
			public Object getMBean() {
				return instance;
			}
		};
	}
}
