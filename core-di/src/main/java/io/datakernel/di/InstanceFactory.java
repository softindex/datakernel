package io.datakernel.di;

public interface InstanceFactory<T> {
	Key<T> key();

	T create();
}
