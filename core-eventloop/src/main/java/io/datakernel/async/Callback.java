package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a universal Callback interface
 */
@FunctionalInterface
public interface Callback<T> {
	/**
	 * Performs action upon of completion of Completable computation
	 */
	void accept(T result, @Nullable Throwable e);

	static <T> Callback<T> toAnotherEventloop(Eventloop anotherEventloop, Callback<T> cb) {
		return (result, e) -> anotherEventloop.execute(() -> cb.accept(result, e));
	}
}
