package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.functional.Try;

import java.util.concurrent.*;
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
	 *
	 * @param value result of Stage
	 */
	static <T> Stage<T> of(@Nullable T value) {
		SettableStage<T> stage = new SettableStage<>(true);
		stage.result = value;
		return stage;
	}

	/**
	 * Creates exceptionally completed {@code Stage}
	 *
	 * @param throwable Exception
	 */
	static <T> Stage<T> ofException(Throwable throwable) {
		SettableStage<T> stage = new SettableStage<>(true);
		stage.result = null;
		stage.exception = throwable;
		return stage;
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
	 * Creates a {@code Stage} wrapper around default java {@code CompletionStage} and runs it immediately.
	 *
	 * @return result of the given completionStage wrapped in a {@code Stage}
	 */
	static <T> Stage<T> ofCompletionStage(CompletionStage<? extends T> completionStage) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<T> stage = SettableStage.create();
		completionStage.whenCompleteAsync((result, throwable) -> {
			stage.set(result, throwable);
			eventloop.completeExternalTask();
		}, eventloop);
		return stage;
	}

	/**
	 * Creates a {@code Stage} wrapper around default java {@code Future} and runs it immediately.
	 *
	 * @return result of the given future wrapped in a {@code Stage}
	 */
	static <T> Stage<T> ofFuture(Executor executor, Future<? extends T> future) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<T> stage = SettableStage.create();
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
	 * Creates a {@code Stage} wrapper around default java {@code Callable} and runs it immediately.
	 *
	 * @return result of the given callable wrapped in a {@code Stage}
	 */
	static <T> Stage<T> ofCallable(Executor executor, Callable<? extends T> callable) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<T> stage = SettableStage.create();
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
	 * Creates a {@code Stage} wrapper around default java {@code Runnable} and runs it immediately.
	 *
	 * @return result of the given runnable wrapped in a {@code Stage}
	 */
	static Stage<Void> ofRunnable(Executor executor, Runnable runnable) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		SettableStage<Void> stage = SettableStage.create();
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
					// checked exception should never happen in Runnables, but added for consistency
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

	@FunctionalInterface
	interface Handler<T, U> {
		interface Completion<T> {
			void complete(@Nullable T result);

			void completeExceptionally(Throwable throwable);

			default void complete(@Nullable T result, Throwable throwable) {
				if (throwable == null) {
					complete(result);
				} else {
					completeExceptionally(throwable);
				}
			}
		}

		void handle(@Nullable T result, @Nullable Throwable throwable, Completion<U> stage);
	}

	<U> Stage<U> handle(Handler<? super T, U> handler);

	<U> Stage<U> handleAsync(Handler<? super T, U> handler);

	<U> Stage<U> handleAsync(Handler<? super T, U> handler, Executor executor);

	/**
	 * Adds given stage to the chain of stages and returns it.
	 *
	 * @param stage given stage
	 * @param <U>   type of result
	 * @param <S>   type of the next stage in the chain
	 * @return subscribed {@code Stage}
	 * @see NextStage
	 * @see io.datakernel.async.AbstractStage.HandlerStage
	 */
	<U, S extends StageConsumer<? super T> & Stage<U>> Stage<U> then(S stage);

	/**
	 * Applies fn to the result of current {@code Stage} when it completes.
	 *
	 * @param fn function to apply
	 * @return {@code Stage} that will apply given function
	 */
	<U> Stage<U> thenApply(Function<? super T, ? extends U> fn);

	/**
	 * Adds new {@code Stage} to the chain. This stage will be executed only if current {@code Stage} completes successfully.
	 *
	 * @param action to be executed
	 * @return this stage
	 */
	Stage<T> thenAccept(Consumer<? super T> action);

	/**
	 * Adds new {@code Stage} to the chain. This stage will be executed only if current {@code Stage} completes successfully.
	 *
	 * @param action to be executed
	 * @return this stage
	 */
	Stage<T> thenRun(Runnable action);

	/**
	 * Apply function to the result of the given stage. This stage will be completed when function completes.
	 *
	 * @param fn to be applied
	 * @return this stage
	 */
	<U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn);

	/**
	 * Adds new {@code Stage} to the chain. Added {@code Stage} will be executed only if this {@code Stage} completes successfully.
	 *
	 * @param action to be executed
	 * @return this {@code Stage}
	 */
	Stage<T> whenComplete(StageConsumer<? super T> action);

	/**
	 * Adds new {@code Stage} to the chain. Added {@code Stage} will be executed only if this {@code Stage} completes exceptionally.
	 *
	 * @param action to be executed
	 * @return this {@code Stage}
	 */
	Stage<T> whenException(Consumer<? super Throwable> action);

	/**
	 * Adds new {@code Stage} to the chain. Added {@code Stage} will apply fn to the {@code Throwable}.
	 *
	 * @param fn to be applied
	 * @return result of fn application if this {@code Stage} completes exceptionally and result of this {@code Stage} otherwise
	 */
	Stage<T> exceptionally(Function<? super Throwable, ? extends T> fn);

	Stage<T> mapFailure(Function<Throwable, Throwable> fn);

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
	 * Discards result of {@code Stage} that completes when this {@code Stage} completes.
	 */
	Stage<Void> toVoid();

	AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException("Stage timeout");

	/**
	 * Wraps this {@code Stage} into a new one, that will complete exceptionally if this {@code Stage} is not completed after timeout.
	 *
	 * @param timeout timeout in milliseconds
	 */
	Stage<T> timeout(long timeout);

	/**
	 * When this stage is completed it's completion will be postponed to the next eventloop tick.
	 */
	Stage<T> post();

	Stage<Try<T>> toTry();

	/**
	 * Wraps {@code Stage} into {@code CompletableFuture}
	 */
	CompletableFuture<T> toCompletableFuture();
}