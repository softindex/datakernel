package io.datakernel.util;

import io.datakernel.annotation.Nullable;

@FunctionalInterface
public interface ThrowingSupplier<T> {
	@Nullable
	T get() throws Throwable;
}
