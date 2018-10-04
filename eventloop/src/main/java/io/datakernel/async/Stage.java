/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Replacement of default java CompletionStage interface.
 *
 * @see SettableStage
 */
public interface Stage<T> {
	/**
	 * Creates successfully completed {@code Stage}
	 */
	@SuppressWarnings("unchecked")
	static CompleteNullStage<Void> complete() {
		return (CompleteNullStage<Void>) CompleteNullStage.INSTANCE;
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
	static <T> CompleteExceptionallyStage<T> ofException(Throwable exception) {
		return new CompleteExceptionallyStage<>(exception);
	}

	static <T> Stage<T> ofCallback(Consumer<SettableStage<T>> callbackConsumer) {
		SettableStage<T> cb = new SettableStage<>();
		callbackConsumer.accept(cb);
		return cb;
	}

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	static <T> Stage<T> ofOptional(Optional<T> optional) {
		return optional.<Stage<T>>map(Stage::of).orElseGet(() -> Stage.ofException(new NoSuchElementException()));
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
	 * Similarly to {@link Try#wrap}, executes given computation and wraps it's result or error in a stage.
	 * Useful for bridging with non-Stage code.
	 *
	 * @param computation a block of code which may throw an exception.
	 */
	static <T> Stage<T> compute(Supplier<T> computation) {
		try {
			return of(computation.get());
		} catch (UncheckedException u) {
			return ofException(u.getCause());
		}
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
				} catch (UncheckedException u) {
					eventloop.execute(() -> stage.setException(u.getCause()));
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
	static Stage<Void> ofRunnable(Executor executor, Runnable runnable) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<Void> stage = new SettableStage<>();
		try {
			executor.execute(() -> {
				try {
					runnable.run();
					eventloop.execute(() -> stage.set(null));
				} catch (UncheckedException u) {
					eventloop.execute(() -> stage.setException(u.getCause()));
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
	default Try<T> asTry() {
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

	default MaterializedStage<T> post() {
		SettableStage<T> result = new SettableStage<>();
		whenComplete(result::post);
		return result;
	}

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

	default <U> Stage<U> thenCallback(BiConsumer<? super T, SettableStage<U>> fn) {
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
	 * @param timeout timeout as Duration
	 */
	Stage<T> timeout(@Nullable Duration timeout);

	/**
	 * Wraps {@code Stage} into {@code CompletableFuture}
	 */
	CompletableFuture<T> toCompletableFuture();
}
