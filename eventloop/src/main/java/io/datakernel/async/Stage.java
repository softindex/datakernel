package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.functional.Try;
import io.datakernel.util.ThrowingRunnable;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Replacement of default java CompletionStage interface.
 *
 * @see SettableStage
 */
public interface Stage<T> {
	/**
	 * Creates successfully completed {@code Stage}
	 */
	static CompleteNullStage<Void> complete() {
		return CompleteNullStage.instance();
	}

	/**
	 * Creates successfully completed {@code Stage}
	 *
	 * @param value result of Stage
	 */
	static <T> CompleteStage<T> of(@Nullable T value) {
		return value == null ? CompleteNullStage.instance() : new CompleteResultStage<>(value);
	}

	/**
	 * Creates exceptionally completed {@code Stage}
	 *
	 * @param exception Throwable
	 */
	static <T> Stage<T> ofException(Throwable exception) {
		return new CompleteExceptionallyStage<>(exception);
	}

	static <T> Stage<T> ofCallback(Consumer<SettableStage<T>> callbackConsumer) {
		SettableStage<T> cb = new SettableStage<>();
		callbackConsumer.accept(cb);
		return cb;
	}

	/**
	 * Creates a completed stage from value and throwable variables.
	 * Useful for {@link #thenComposeEx(BiFunction)} passthroughs (eg. when mapping specific exceptions).
	 *
	 * @param value     value to wrap when exception is null
	 * @param exception possibly-null exception, determines type of stage completion
	 */
	static <T> Stage<T> of(@Nullable T value, @Nullable Throwable exception) {
		assert !(value != null && exception != null);
		return exception == null ? of(value) : ofException(exception);
	}

	/**
	 * Creates a {@code Stage} wrapper around default java {@code CompletableFuture} and runs it immediately.
	 *
	 * @return result of the given future wrapped in a {@code Stage}
	 */
	static <T> Stage<T> ofFuture(CompletableFuture<? extends T> future) {
		return ofCompletionStage(future);
	}

	/**
	 * Wraps Java {@code CompletionStage} in a {@code Stage}, running it in current eventloop.
	 *
	 * @param completionStage completion stage itself
	 * @return result of the given completionStage wrapped in a {@code Stage}
	 */
	static <T> Stage<T> ofCompletionStage(CompletionStage<? extends T> completionStage) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<T> stage = new SettableStage<>();
		completionStage.whenCompleteAsync((result, throwable) -> {
			stage.set(result, throwable);
			eventloop.completeExternalTask();
		}, eventloop);
		return stage;
	}

	/**
	 * Wraps Java {@code Future} in a {@code Stage} running it with given executor.
	 *
	 * @param executor executor to execute the future concurrently
	 * @param future   the future itself
	 * @return result of the future wrapped in a {@code Stage}
	 */
	static <T> Stage<T> ofFuture(Executor executor, Future<? extends T> future) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<T> stage = new SettableStage<>();
		try {
			executor.execute(() -> {
				try {
					T value = future.get();
					eventloop.execute(() -> stage.set(value));
				} catch (ExecutionException e) {
					eventloop.execute(() -> stage.setException(e.getCause()));
				} catch (InterruptedException e) {
					eventloop.execute(() -> stage.setException(e));
				} catch (Throwable e) {
					eventloop.execute(() -> eventloop.recordFatalError(e, future));
				} finally {
					eventloop.completeExternalTask();
				}
			});
		} catch (RejectedExecutionException e) {
			eventloop.completeExternalTask();
			stage.setException(e);
		}
		return stage;
	}

	/**
	 * Runs some task in other thread (executed by a given {@code Executor}) and returns a {@code Stage} for it.
	 * Also manages external task count for current eventloop, so it wont shut down until the task is complete.
	 *
	 * @param executor executor to execute the task concurrently
	 * @param callable the task itself
	 * @return {@code Stage} for the given task
	 */
	static <T> Stage<T> ofCallable(Executor executor, Callable<? extends T> callable) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<T> stage = new SettableStage<>();
		try {
			executor.execute(() -> {
				try {
					T result = callable.call();
					eventloop.execute(() -> stage.set(result));
				} catch (CompletionException e) {
					eventloop.execute(() -> stage.setException(e));
				} catch (RuntimeException e) {
					eventloop.execute(() -> eventloop.recordFatalError(e, callable));
				} catch (Exception e) {
					eventloop.execute(() -> stage.setException(e));
				} catch (Throwable e) {
					eventloop.execute(() -> eventloop.recordFatalError(e, callable));
				} finally {
					eventloop.completeExternalTask();
				}
			});
		} catch (RejectedExecutionException e) {
			eventloop.completeExternalTask();
			stage.setException(e);
		}
		return stage;
	}

	/**
	 * Same as {@link #ofCallable(Executor, Callable)}, but without a result (returned stage is only a marker of completion).
	 */
	static Stage<Void> ofThrowingRunnable(Executor executor, ThrowingRunnable runnable) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<Void> stage = new SettableStage<>();
		try {
			executor.execute(() -> {
				try {
					runnable.run();
					eventloop.execute(() -> stage.set(null));
				} catch (CompletionException e) {
					eventloop.execute(() -> stage.setException(e));
				} catch (RuntimeException e) {
					eventloop.execute(() -> eventloop.recordFatalError(e, runnable));
				} catch (Exception e) {
					eventloop.execute(() -> stage.setException(e));
				} catch (Throwable e) {
					eventloop.execute(() -> eventloop.recordFatalError(e, runnable));
				} finally {
					eventloop.completeExternalTask();
				}
			});
		} catch (RejectedExecutionException e) {
			eventloop.completeExternalTask();
			stage.setException(e);
		}
		return stage;
	}

	/**
	 * Adapter for {@link #ofThrowingRunnable(Executor, ThrowingRunnable)}
	 */
	static Stage<Void> ofRunnable(Executor executor, Runnable runnable) {
		return ofThrowingRunnable(executor, runnable::run);
	}

	default boolean isComplete() {
		return isResult() || isException();
	}

	boolean isResult();

	boolean isException();

	default boolean isMaterialized() {
		return this instanceof MaterializedStage;
	}

	default boolean hasResult() {
		return isResult() && isMaterialized();
	}

	default boolean hasException() {
		return isException() && isMaterialized();
	}

	T getResult();

	Throwable getException();

	@Nullable
	default Try<T> getTry() {
		if (hasResult()) return Try.of(getResult());
		else if (hasException()) return Try.ofException(getException());
		else return null;
	}

	default boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		if (hasResult()) {
			consumer.accept(getResult(), null);
			return true;
		} else if (hasException()) {
			consumer.accept(null, getException());
			return true;
		} else {
			return false;
		}
	}

	default boolean setResultTo(Consumer<? super T> consumer) {
		if (hasResult()) {
			consumer.accept(getResult());
			return true;
		} else {
			return false;
		}
	}

	default boolean setExceptionTo(Consumer<Throwable> consumer) {
		if (hasException()) {
			consumer.accept(getException());
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Ensures that stage completes asynchronously:
	 * If this stage is already complete, its completion will be posted to next eventloop tick.
	 * Otherwise, do nothing.
	 */
	Stage<T> async();

	default MaterializedStage<T> materialize() {
		if (isMaterialized()) return (MaterializedStage<T>) this;
		SettableStage<T> cb = new SettableStage<>();
		whenComplete(cb::set);
		return cb;
	}

	/**
	 * Executes given stage after execution of this stage completes
	 *
	 * @param stage given stage
	 * @param <U>   type of result
	 * @return subscribed {@code Stage}
	 */
	<U, S extends BiConsumer<? super T, Throwable> & Stage<U>> Stage<U> then(S stage);

	/**
	 * Applies fn to the result of this {@code Stage}
	 *
	 * @param fn function to apply
	 * @return {@code Stage} that will apply given function
	 */
	<U> Stage<U> thenApply(Function<? super T, ? extends U> fn);

	/**
	 * Applies fn to the result or exception of this {@code Stage}
	 *
	 * @param fn function to apply
	 * @return {@code Stage} that will apply given function
	 */
	<U> Stage<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn);

	/**
	 * Runs action after successful completion of this stage
	 *
	 * @param action to be executed
	 * @return this stage
	 */
	Stage<T> thenRun(Runnable action);

	/**
	 * Runs action after completion of this stage
	 *
	 * @param action to be executed
	 * @return this stage
	 */
	Stage<T> thenRunEx(Runnable action);

	/**
	 * Applies function to the result of this stage if it completes successfully.
	 * Returned stage will be completed when stage returned from function completes.
	 *
	 * @param fn to be applied
	 */
	<U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn);

	/**
	 * Applies function to the result of this stage.
	 * Returned stage will be completed when stage returned from function completes.
	 *
	 * @param fn to be applied
	 * @return this stage
	 */
	<U> Stage<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Stage<U>> fn);

	default <U> Stage<U> thenCallback(BiConsumer<? super T, Callback<U>> fn) {
		return thenCompose(value -> {
			SettableStage<U> cb = new SettableStage<>();
			fn.accept(value, cb);
			return cb;
		});
	}

	/**
	 * Subscribes given action to be executed after this stage completes
	 *
	 * @param action to be executed
	 * @return this {@code Stage}
	 */
	Stage<T> whenComplete(BiConsumer<? super T, Throwable> action);

	/**
	 * Subscribes given action to be executed after this stage completes successfully
	 *
	 * @param action to be executed
	 * @return this {@code Stage}
	 */
	Stage<T> whenResult(Consumer<? super T> action);

	/**
	 * Subscribes given action to be executed after this stage completes exceptionally
	 *
	 * @param action to be executed
	 * @return this {@code Stage}
	 */
	Stage<T> whenException(Consumer<Throwable> action);

	Stage<T> thenException(Function<? super T, Throwable> fn);

	@FunctionalInterface
	interface ThrowingFunction<T, R> {
		R apply(T t) throws Exception;
	}

	default <U> Stage<U> thenTry(ThrowingFunction<? super T, ? extends U> fn) {
		return thenCompose(result -> {
			try {
				return Stage.of(fn.apply(result));
			} catch (Throwable throwable) {
				return Stage.ofException(throwable);
			}
		});
	}

	/**
	 * Combines two {@code Stage} in one using fn.
	 *
	 * @param other {@code Stage} to combine
	 * @param fn    function to combine results of both stages into one
	 * @return {@code Stage} that completes when fn was applied on the result of both stages
	 */
	<U, V> Stage<V> combine(Stage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn);

	/**
	 * Combines two {@code Stage} in one and completes when both have been completed.
	 *
	 * @param other {@code Stage} to combine
	 */
	Stage<Void> both(Stage<?> other);

	/**
	 * Combines two {@code Stage} in one.
	 *
	 * @param other {@code Stage} to combine
	 * @return result of the first completed {@code Stage}
	 */
	Stage<T> either(Stage<? extends T> other);

	/**
	 * Returns stage that always completes successfully with result or exception wrapped in Try
	 *
	 * @see Try
	 */
	Stage<Try<T>> toTry();

	/**
	 * Waits for result and discard it.
	 */
	Stage<Void> toVoid();

	AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException("Stage timeout");

	/**
	 * Returns stage that completes successfully if this stage completes before timeout.
	 * Otherwise it completes with timeout exception.
	 * If <code>null</code> is given, no timeout is applied.
	 *
	 * @param timeout timeout in milliseconds
	 */
	Stage<T> timeout(@Nullable Duration timeout);

	/**
	 * Wraps {@code Stage} into {@code CompletableFuture}
	 */
	CompletableFuture<T> toCompletableFuture();
}
