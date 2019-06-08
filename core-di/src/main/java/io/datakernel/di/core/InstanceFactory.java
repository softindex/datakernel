package io.datakernel.di.core;

public interface InstanceFactory<T> {
	Key<T> key();

	T create();
}
