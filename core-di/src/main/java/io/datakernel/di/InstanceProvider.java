package io.datakernel.di;

public interface InstanceProvider<T> {
	T provide();

	T create();
}
