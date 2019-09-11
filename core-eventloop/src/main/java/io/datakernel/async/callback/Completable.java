package io.datakernel.async.callback;

import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * An abstraction over something, which successful or exceptional
 * completion at some point in the future can be listened to.
 * <p>
 * This is a more generic and flexible abstraction over
 * things like completion stages, promises or futures
 * @param <T>
 */
public interface Completable<T> {
	/**
	 * Subscribes given action to be executed
	 * after this {@code Completable} computation completes
	 *
	 * @param action to be executed
	 */
	void onComplete(@Async.Schedule @NotNull Callback<? super T> action);

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
