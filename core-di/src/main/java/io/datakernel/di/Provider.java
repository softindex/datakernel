package io.datakernel.di;

public interface Provider<T> {
	T provideNew();

	T provideSingleton();
}
