package io.datakernel.di;

public interface InstanceInjector<T> {
	void inject(T existingInstance);
}
