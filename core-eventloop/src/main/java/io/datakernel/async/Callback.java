package io.datakernel.async;

import org.jetbrains.annotations.Nullable;

/**
 * Represents a universal Callback interface
 */
@FunctionalInterface
public interface Callback<T> {
	/**
	 * Performs action upon of completion of Completable computation
	 */
//	@Async.Execute
	void accept(T result, @Nullable Throwable e);
}
