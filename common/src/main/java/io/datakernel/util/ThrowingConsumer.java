package io.datakernel.util;

import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingConsumer<T> {
	void accept(T item) throws Throwable;
}
