package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.util.function.Function;

@FunctionalInterface
public interface StageConsumer<T> {
	void accept(@Nullable T result, @Nullable Throwable error);

	static <T, R> StageConsumer<R> transform(Function<? super R, ? extends T> fn, StageConsumer<? super T> toConsumer) {
		return (value, throwable) -> toConsumer.accept(value != null ? fn.apply(value) : null, throwable);
	}
}
