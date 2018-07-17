package io.datakernel.util;

@FunctionalInterface
public interface ThrowingConsumer<T> {
	void accept(T item) throws Throwable;
}
