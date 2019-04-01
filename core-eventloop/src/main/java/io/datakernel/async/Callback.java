package io.datakernel.async;

import org.jetbrains.annotations.Nullable;

/**
 * Represents a universal Callback interface
 */
@FunctionalInterface
public interface Callback<T> {
	/**
	 * Performs this operation on the given arguments
	 */
	void accept(T result, @Nullable Throwable e);
}
