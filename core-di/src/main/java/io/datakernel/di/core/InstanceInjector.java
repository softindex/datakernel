package io.datakernel.di.core;

public interface InstanceInjector<T> {
	Key<T> key();

	void injectInto(T existingInstance);
}
