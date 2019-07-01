package io.datakernel.async;

import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

public interface Completable<T> {
	/**
	 * Subscribes given action to be executed
	 * after this {@code Completable} computation completes
	 *
	 * @param action to be executed
	 */
	void onComplete(@NotNull Callback<? super T> action);

	/**
	 * Subscribes given action to be executed
	 * after this {@code Completable} computation completes
	 *
	 * @param action to be executed
	 */
	default void onComplete(Runnable action) {
		onComplete((result, e) -> action.run());
	}

	/**
	 * Subscribes given action to be executed after
	 * this {@code Completable} computation completes successfully
	 *
	 * @param action to be executed
	 */
	default void onResult(@NotNull Consumer<? super T> action) {
		onComplete((result, e) -> {
			if (e == null) {
				action.accept(result);
			}
		});
	}

	/**
	 * Subscribes given action to be executed after
	 * this {@code Completable} computation completes exceptionally
	 *
	 * @param action to be executed
	 */
	default void onException(@NotNull Consumer<@NotNull Throwable> action) {
		onComplete((result, e) -> {
			if (e != null) {
				action.accept(e);
			}
		});
	}
}
