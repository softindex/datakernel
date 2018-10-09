package io.datakernel.async;

import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class CompleteExceptionallyStage<T> implements MaterializedStage<T> {
	private final Throwable exception;

	public CompleteExceptionallyStage(Throwable exception) {
		this.exception = exception;
	}

	@Override
	public boolean isComplete() {
		return true;
	}

	@Override
	public boolean isResult() {
		return false;
	}

	@Override
	public boolean isException() {
		return true;
	}

	@Override
	public boolean hasResult() {
		return false;
	}

	@Override
	public boolean hasException() {
		return true;
	}

	@Override
	public T getResult() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Throwable getException() {
		return exception;
	}

	@Override
	public Try<T> asTry() {
		return Try.ofException(exception);
	}

	@Override
	public boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		consumer.accept(null, exception);
		return true;
	}

	@Override
	public boolean setResultTo(Consumer<? super T> consumer) {
		return false;
	}

	@Override
	public boolean setExceptionTo(Consumer<Throwable> consumer) {
		consumer.accept(exception);
		return true;
	}

	@SuppressWarnings("unchecked")
	public <U> CompleteExceptionallyStage<U> mold() {
		return (CompleteExceptionallyStage<U>) this;
	}

	@Override
	public <U, S extends BiConsumer<? super T, Throwable> & Stage<U>> Stage<U> then(S stage) {
		stage.accept(null, exception);
		return stage;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Stage<U> thenApply(Function<? super T, ? extends U> fn) {
		return (CompleteExceptionallyStage<U>) this;
	}

	@Override
	public <U> Stage<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		try {
			return Stage.of(fn.apply(null, exception));
		} catch (UncheckedException u) {
			return Stage.ofException(u.getCause());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn) {
		return (CompleteExceptionallyStage<U>) this;
	}

	@Override
	public <U> Stage<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Stage<U>> fn) {
		try {
			return fn.apply(null, exception);
		} catch (UncheckedException u) {
			return Stage.ofException(u.getCause());
		}
	}

	@Override
	public Stage<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		action.accept(null, exception);
		return this;
	}

	@Override
	public Stage<T> whenResult(Consumer<? super T> action) {
		return this;
	}

	@Override
	public Stage<T> whenException(Consumer<Throwable> action) {
		action.accept(exception);
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<T> thenException(Function<? super T, Throwable> fn) {
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U, V> Stage<V> combine(Stage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		return (CompleteExceptionallyStage<V>) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<Void> both(Stage<?> other) {
		return (CompleteExceptionallyStage<Void>) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<T> either(Stage<? extends T> other) {
		return (Stage<T>) other;
	}

	@Override
	public MaterializedStage<T> async() {
		SettableStage<T> result = new SettableStage<>();
		getCurrentEventloop().post(() -> result.setException(exception));
		return result;
	}

	@Override
	public Stage<Try<T>> toTry() {
		return Stage.of(Try.ofException(exception));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<Void> toVoid() {
		return (CompleteExceptionallyStage<Void>) this;
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(exception);
		return future;
	}
}
