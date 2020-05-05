package io.datakernel.async.callback;

import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.eventloop.util.RunnableWithContext.wrapContext;

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
		return (result, e) -> anotherEventloop.execute(wrapContext(cb, () -> cb.accept(result, e)));
	}
}
