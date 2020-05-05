package io.datakernel.eventloop.util;

import io.datakernel.common.ApplicationSettings;

public interface RunnableWithContext extends Runnable {
	boolean WRAP_CONTEXT = ApplicationSettings.getBoolean(RunnableWithContext.class, "wrapContext", false);

	Object getContext();

	static RunnableWithContext of(Object context, Runnable runnable) {
		return new RunnableWithContext() {
			@Override
			public Object getContext() {
				return context;
			}

			@Override
			public void run() {
				runnable.run();
			}

			@Override
			public String toString() {
				return "RunnableWithContext{" +
						"runnable=" + runnable +
						", context=" + context +
						'}';
			}
		};
	}

	static Runnable wrapContext(Object context, Runnable runnable) {
		return WRAP_CONTEXT ? of(context, runnable) : runnable;
	}

}
