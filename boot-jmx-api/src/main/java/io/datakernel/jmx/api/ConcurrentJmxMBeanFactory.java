package io.datakernel.jmx.api;

public final class ConcurrentJmxMBeanFactory implements JmxWrapperFactory {
	@Override
	public MBeanWrapper wrap(Object instance) {
		return new MBeanWrapper() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}

			@Override
			public Object getMBean() {
				return instance;
			}
		};
	}
}
