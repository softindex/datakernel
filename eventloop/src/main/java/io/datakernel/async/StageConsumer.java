package io.datakernel.async;

import io.datakernel.annotation.Nullable;

@FunctionalInterface
public interface StageConsumer<T> {
	void accept(@Nullable T result, @Nullable Throwable error);
}
