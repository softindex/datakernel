package io.datakernel.di;

public interface InstanceProvider<T> {
	Key<T> key();

	T get();
}
