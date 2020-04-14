package io.datakernel.jmx.api;

public interface MBeanWrapper {
	void execute(Runnable command);

	Object getMBean();
}
