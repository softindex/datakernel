/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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
 */

package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;
import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Replaces default Java {@link CompletionStage} interface.
 * Each promise represents some sort of operations executed
 * after the previous {@code Promise} completes.
 *
 * {@code Promise} can complete either successfully with a result
 * which will be wrapped inside the {@code Promise} or exceptionally,
 * returning a new {@link CompletePromise} or {@link CompleteExceptionallyPromise}
 * respectively.
 *
 * {@link SettablePromise} allows to create a root for chain of {@code Promise}s.
 */
public interface Promise<T> {
	/**
	 * Creates successfully completed {@code Promise}
	 */
	@NotNull
	@SuppressWarnings("unchecked")
	static CompleteNullPromise<Void> complete() {
		return (CompleteNullPromise<Void>) CompleteNullPromise.INSTANCE;
	}

	/**
	 * Creates successfully completed {@code Promise}.
	 *
	 * @param value result of Promise. If value is {@code null},
	 *              returns {@link CompleteNullPromise}, otherwise
	 *              {@link CompleteResultPromise}
	 */
	@NotNull
	static <T> CompletePromise<T> of(@Nullable T value) {
		return value == null ? CompleteNullPromise.instance() : new CompleteResultPromise<>(value);
	}

	/**
	 * Creates an exceptionally completed {@code Promise}
	 *
	 * @param e Throwable
	 */
	@NotNull
	static <T> CompleteExceptionallyPromise<T> ofException(@NotNull Throwable e) {
		return new CompleteExceptionallyPromise<>(e);
	}

	@NotNull
	static <T> Promise<T> ofCallback(@NotNull Consumer<SettablePromise<T>> callbackConsumer) {
		SettablePromise<T> cb = new SettablePromise<>();
		try {
			callbackConsumer.accept(cb);
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
		return cb;
	}

	@NotNull
	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	static <T> Promise<T> ofOptional(@NotNull Optional<T> optional) {
		return ofOptional(optional, NoSuchElementException::new);
	}

	@NotNull
	@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "OptionalIsPresent"})
	static <T> Promise<T> ofOptional(@NotNull Optional<T> optional, @NotNull Supplier<? extends Throwable> errorSupplier) {
		if (optional.isPresent()) return Promise.of(optional.get());
		return Promise.ofException(errorSupplier.get());
	}

	/**
	 * Creates a completed promise from value and throwable variables.
	 * Useful for {@link #thenComposeEx(BiFunction)} passthroughs
	 * (e.g. when mapping specific exceptions).
	 *
	 * @param value value to wrap when exception is null
	 * @param e     possibly-null exception, determines type of promise completion
	 */
	@NotNull
	static <T> Promise<T> of(@Nullable T value, @Nullable Throwable e) {
		assert !(value != null && e != null);
		return e == null ? of(value) : ofException(e);
	}

	@NotNull
	static <T> Promise<T> ofTry(@NotNull Try<T> t) {
		return t.reduce(Promise::of, Promise::ofException);
	}

	/**
	 * Creates a {@code Promise} wrapper around default java {@code CompletableFuture} and runs it immediately.
	 *
	 * @return result of the given future wrapped in a {@code Promise}
	 */
	@NotNull
	static <T> Promise<T> ofFuture(@NotNull CompletableFuture<? extends T> future) {
		return ofCompletionStage(future);
	}

	/**
	 * Wraps Java {@code CompletionStage} in a {@code Promise}, running it in current eventloop.
	 *
	 * @param completionStage completion stage itself
	 * @return result of the given completionStage wrapped in a {@code Promise}
	 */
	@NotNull
	static <T> Promise<T> ofCompletionStage(@Async.Schedule @NotNull CompletionStage<? extends T> completionStage) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettablePromise<T> promise = new SettablePromise<>();
		completionStage.whenCompleteAsync(new BiConsumer<T, Throwable>() {
			private void complete(@SuppressWarnings("unused") @Async.Execute CompletionStage<? extends T> $, T result, Throwable e) {
				promise.set(result, e);
				eventloop.completeExternalTask();
			}

			@Override
			public void accept(T result, Throwable e) {
				complete(completionStage, result, e);
			}
		}, eventloop);
		return promise;
	}

	/**
	 * Wraps Java {@code Future} in a {@code Promise} running it with given executor.
	 *
	 * @param executor executor to execute the future concurrently
	 * @param future   the future itself
	 * @return result of the future wrapped in a {@code Promise}
	 */
	@NotNull
	static <T> Promise<T> ofFuture(@NotNull Executor executor, @Async.Schedule @NotNull Future<? extends T> future) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettablePromise<T> promise = new SettablePromise<>();
		try {
			executor.execute(new Runnable() {
				private void run(@Async.Execute Future<? extends T> future) {
					try {
						T value = future.get();
						eventloop.execute(() -> promise.set(value));
					} catch (ExecutionException e) {
						eventloop.execute(() -> promise.setException(e.getCause()));
					} catch (InterruptedException e) {
						eventloop.execute(() -> promise.setException(e));
					} catch (Throwable e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, future));
					} finally {
						eventloop.completeExternalTask();
					}
				}

				@Override
				public void run() {
					run(future);
				}
			});
		} catch (RejectedExecutionException e) {
			eventloop.completeExternalTask();
			promise.setException(e);
		}
		return promise;
	}

	/**
	 * Runs some task in other thread (executed by a given {@code Executor})
	 * and returns a {@code Promise} for it. Also manages external task count
	 * for current eventloop, so it won't shut down until the task is complete.
	 *
	 * @param executor executor to execute the task concurrently
	 * @param callable the task itself
	 * @return {@code Promise} for the given task
	 */
	static <T> Promise<T> ofCallable(@NotNull Executor executor, @Async.Schedule @NotNull Callable<? extends T> callable) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettablePromise<T> promise = new SettablePromise<>();
		try {
			executor.execute(new Runnable() {
				private void run(@Async.Execute Callable<? extends T> callable) {
					try {
						T result = callable.call();
						eventloop.execute(() -> promise.set(result));
					} catch (UncheckedException u) {
						eventloop.execute(() -> promise.setException(u.getCause()));
					} catch (RuntimeException e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, callable));
					} catch (Exception e) {
						eventloop.execute(() -> promise.setException(e));
					} catch (Throwable e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, callable));
					} finally {
						eventloop.completeExternalTask();
					}
				}

				@Override
				public void run() {
					run(callable);
				}
			});
		} catch (RejectedExecutionException e) {
			eventloop.completeExternalTask();
			promise.setException(e);
		}
		return promise;
	}

	/**
	 * Same as {@link #ofCallable(Executor, Callable)}, but without a result
	 * (returned {@code Promise} is only a marker of completion).
	 */
	@NotNull
	static Promise<Void> ofRunnable(@NotNull Executor executor, @Async.Schedule @NotNull Runnable runnable) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettablePromise<Void> promise = new SettablePromise<>();
		try {
			executor.execute(new Runnable() {
				private void run(@Async.Execute Runnable runnable) {
					try {
						runnable.run();
						eventloop.execute(() -> promise.set(null));
					} catch (UncheckedException u) {
						eventloop.execute(() -> promise.setException(u.getCause()));
					} catch (Throwable e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, runnable));
					} finally {
						eventloop.completeExternalTask();
					}
				}

				@Override
				public void run() {
					run(runnable);
				}
			});
		} catch (RejectedExecutionException e) {
			eventloop.completeExternalTask();
			promise.setException(e);
		}
		return promise;
	}

	@Contract(pure = true)
	default boolean isComplete() {
		return isResult() || isException();
	}

	@Contract(pure = true)
	boolean isResult();

	@Contract(pure = true)
	boolean isException();

	/**
	 * Ensures that {@code Promise} completes asynchronously:
	 * if this promise is already complete, its completion
	 * will be posted to next eventloop tick.
	 * Otherwise, does nothing.
	 */
	@Contract(pure = true)
	@NotNull
	Promise<T> async();

	@Contract(pure = true)
	@NotNull
	default MaterializedPromise<T> post() {
		SettablePromise<T> result = new SettablePromise<>();
		whenComplete(result::post);
		return result;
	}

	@Contract(pure = true)
	@NotNull
	default MaterializedPromise<T> materialize() {
		assert !isComplete() : "Trying to materialize a completed promise";
		SettablePromise<T> cb = new SettablePromise<>();
		whenComplete(cb::set);
		return cb;
	}

	/**
	 * Executes given promise after execution of this promise completes.
	 *
	 * @param promise given promise
	 * @param <U>     type of result
	 * @return subscribed {@code Promise}
	 */
	@Contract("_ -> param1")
	@NotNull <U, P extends BiConsumer<? super T, Throwable> & Promise<U>> Promise<U> then(@NotNull P promise);

	/**
	 * Applies {@code fn} to the result of this {@code Promise}.
	 *
	 * @param fn function to apply
	 * @return {@code Promise} that will apply given function
	 */
	@Contract(pure = true)
	@NotNull <U> Promise<U> thenApply(@NotNull Function<? super T, ? extends U> fn);

	/**
	 * Applies {@code fn} to the result or exception of this {@code Promise}.
	 *
	 * @param fn function to apply
	 * @return {@code Promise} that will apply given function
	 */
	@Contract(pure = true)
	@NotNull <U> Promise<U> thenApplyEx(@NotNull BiFunction<? super T, Throwable, ? extends U> fn);

	/**
	 * Applies function to the result of this promise if it completes successfully.
	 * Returned promise will be completed when promise returned from function completes.
	 *
	 * @param fn to be applied
	 */
	@Contract(pure = true)
	@NotNull <U> Promise<U> thenCompose(@NotNull Function<? super T, ? extends Promise<U>> fn);

	/**
	 * Applies function to the result of this promise.
	 * Returned promise will be completed when promise returned from function completes.
	 *
	 * @param fn to be applied
	 * @return this promise
	 */
	@Contract(pure = true)
	@NotNull <U> Promise<U> thenComposeEx(@NotNull BiFunction<? super T, Throwable, ? extends Promise<U>> fn);

	/**
	 * Subscribes given action to be executed
	 * after this promise completes.
	 *
	 * @param action to be executed
	 * @return this {@code Promise}
	 */
	@Contract(" _ -> this")
	@NotNull
	Promise<T> whenComplete(@NotNull BiConsumer<? super T, Throwable> action);

	/**
	 * Subscribes given action to be executed
	 * after this promise completes successfully.
	 *
	 * @param action to be executed
	 * @return this {@code Promise}
	 */
	@Contract(" _ -> this")
	@NotNull
	Promise<T> whenResult(@NotNull Consumer<? super T> action);

	/**
	 * Subscribes given action to be executed
	 * after this promise completes exceptionally.
	 *
	 * @param action to be executed
	 * @return this {@code Promise}
	 */
	@Contract(" _ -> this")
	@NotNull
	Promise<T> whenException(@NotNull Consumer<Throwable> action);

	/**
	 * Combines two {@code Promise}s in one using {@code fn}.
	 *
	 * @param other {@code Promise} to combine
	 * @param fn    function to combine results of both promises into one
	 * @return {@code Promise} that completes when {@code fn} was applied on the result of both promises
	 */
	@Contract(pure = true)
	@NotNull <U, V>
	Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunction<? super T, ? super U, ? extends V> fn);

	/**
	 * Combines two {@code Promise} in one and completes when both have been completed.
	 *
	 * @param other {@code Promise} to combine
	 */
	@Contract(pure = true)
	@NotNull
	Promise<Void> both(@NotNull Promise<?> other);

	/**
	 * Combines two {@code Promise}s in one.
	 *
	 * @param other {@code Promise} to combine
	 * @return result of the first completed {@code Promise}
	 */
	@Contract(pure = true)
	@NotNull
	Promise<T> either(@NotNull Promise<? extends T> other);

	/**
	 * Returns {@code Promise} that always completes
	 * successfully with result or exception wrapped in Try.
	 *
	 * @see Try
	 */
	@Contract(pure = true)
	@NotNull
	Promise<Try<T>> toTry();

	/**
	 * Waits for result and discards it.
	 */
	@Contract(pure = true)
	@NotNull
	Promise<Void> toVoid();

	/**
	 * Wraps {@code Promise} into {@link CompletableFuture}.
	 */
	@Contract(pure = true)
	@NotNull
	CompletableFuture<T> toCompletableFuture();
}
