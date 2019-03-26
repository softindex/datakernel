package io.datakernel.async;

import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface Callback<T> {
	void accept(T result, @Nullable Throwable e);
}
