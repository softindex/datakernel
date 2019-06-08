package io.datakernel.di;

public interface InstanceInjector<T> {
	Key<T> key();

	void inject(T existingInstance);
}
