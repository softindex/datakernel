package io.datakernel.di.core;

public interface InstanceProvider<T> {
	Key<T> key();

	T get();
}
