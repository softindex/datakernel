package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;

import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Stage<T> {

	static <T> Stage<T> of(T value) {
		SettableStage<T> stage = new SettableStage<>();
		stage.result = value;
		return stage;
	}

	static <T> Stage<T> ofException(Throwable throwable) {
		SettableStage<T> stage = new SettableStage<>();
		stage.result = null;
		stage.exception = throwable;
		return stage;
	}

	static <T> Stage<T> ofFuture(CompletableFuture<? extends T> future) {
		return ofCompletionStage(future);
	}

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
			void complete(T result);

			void completeExceptionally(Throwable throwable);

			default void complete(T result, Throwable throwable) {
				if (throwable == null) {
					complete(result);
				} else {
					completeExceptionally(throwable);
				}
			}
		}

		void handle(T result, Throwable throwable, Completion<U> stage);
	}

	<U> Stage<U> handle(Handler<? super T, U> handler);

	<U> Stage<U> handleAsync(Handler<? super T, U> handler);

	<U> Stage<U> handleAsync(Handler<? super T, U> handler, Executor executor);

	<U, S extends BiConsumer<? super T, ? super Throwable> & Stage<U>> Stage<U> then(S stage);

	<U> Stage<U> thenApply(Function<? super T, ? extends U> fn);

	Stage<T> thenAccept(Consumer<? super T> action);

	Stage<T> thenRun(Runnable action);

	<U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn);

	Stage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

	Stage<T> whenException(Consumer<? super Throwable> action);

	Stage<T> exceptionally(Function<? super Throwable, ? extends T> fn);

	<U, V> Stage<V> combine(Stage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn);

	Stage<Void> both(Stage<?> other);

	Stage<T> either(Stage<? extends T> other);

	Stage<Void> toVoid();

	AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException();

	Stage<T> timeout(long millis);

	Stage<T> post();

	CompletableFuture<T> toCompletableFuture();
}