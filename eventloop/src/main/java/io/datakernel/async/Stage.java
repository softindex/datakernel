package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
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

	static <T> Stage<T> ofFuture(CompletableFuture<T> completableFuture) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		SettableStage<T> stage = SettableStage.create();
		completableFuture.whenComplete((value, throwable) -> eventloop.execute(() -> stage.set(value, throwable)));
		return stage;
	}

	static <T> Stage<T> ofFuture(Future<T> future, Executor executor) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		SettableStage<T> stage = SettableStage.create();
		executor.execute(() -> {
			try {
				T value = future.get();
				eventloop.execute(() -> stage.set(value));
			} catch (InterruptedException | ExecutionException e) {
				eventloop.execute(() -> stage.setException(e));
			}
		});
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

	Stage<T> delay(long millis);

	Stage<T> post();

	CompletableFuture<T> toCompletableFuture();
}