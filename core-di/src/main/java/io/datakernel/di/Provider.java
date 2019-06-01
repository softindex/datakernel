package io.datakernel.di;

public interface Provider<T> {
	T get();

	T newInstance();
}
