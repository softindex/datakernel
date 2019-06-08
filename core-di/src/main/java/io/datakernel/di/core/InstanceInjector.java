package io.datakernel.di.core;

public interface InstanceInjector<T> {
	Key<T> key();

	void inject(T existingInstance);
}
