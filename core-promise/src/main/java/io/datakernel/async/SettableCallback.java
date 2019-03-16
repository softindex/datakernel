package io.datakernel.async;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public interface SettableCallback<T> {
	default boolean isComplete() {
		return isResult() || isException();
	}

	default boolean isResult() {
		return isComplete() && !isException();
	}

	default boolean isException() {
		return isComplete() && !isResult();
	}

	void set(T result);

	void setException(@NotNull Throwable e);

	default void set(T result, @Nullable Throwable e) {
		if (e == null) {
			set(result);
		} else {
			setException(e);
		}
	}

	/**
	 * Tries to set provided {@code result} for this
	 * {@code SettablePromise} if it is not completed yet.
	 */
	default void trySet(T result) {
		if (!isComplete()) {
			set(result);
		}
	}

	/**
	 * Tries to set provided {@code e} exception for this
	 * {@code SettablePromise} if it is not completed yet.
	 */
	default void trySetException(@NotNull Throwable e) {
		if (!isComplete()) {
			setException(e);
		}
	}

	/**
	 * Tries to set result or exception for this {@code SettablePromise}
	 * if it not completed yet. Otherwise does nothing.
	 */
	default void trySet(T result, @Nullable Throwable e) {
		if (!isComplete()) {
			if (e == null) {
				trySet(result);
			} else {
				trySetException(e);
			}
		}
	}

	default void post(T result) {
		getCurrentEventloop().post(() -> set(result));
	}

	default void postException(@NotNull Throwable e) {
		getCurrentEventloop().post(() -> setException(e));
	}

	default void post(T result, @Nullable Throwable e) {
		getCurrentEventloop().post(() -> set(result, e));
	}

	default void tryPost(T result) {
		getCurrentEventloop().post(() -> trySet(result));
	}

	default void tryPostException(@NotNull Throwable e) {
		getCurrentEventloop().post(() -> trySetException(e));
	}

	default void tryPost(T result, @Nullable Throwable e) {
		getCurrentEventloop().post(() -> trySet(result, e));
	}

}
