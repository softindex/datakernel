package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.functional.Try;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class CompleteStage<T> implements MaterializedStage<T> {
	private final T result;
	private final Throwable exception;

	CompleteStage(T result, Throwable exception) {
		this.result = result;
		this.exception = exception;
	}

	public static <T> CompleteStage<T> of(T result, Throwable exception) {
		return new CompleteStage<>(result, exception);
	}

	public static <T> CompleteStage<T> of(T result) {
		return new CompleteStage<>(result, null);
	}

	public static <T> CompleteStage<T> ofException(Throwable e) {
		return new CompleteStage<>(null, e);
	}

	@Override
	public boolean isComplete() {
		return true;
	}

	@Override
	public boolean isResult() {
		return exception == null;
	}

	@Override
	public boolean isException() {
		return exception != null;
	}

	@Override
	public boolean hasResult() {
		return isResult();
	}

	@Override
	public boolean hasException() {
		return isException();
	}

	@Override
	public T getResult() {
		assert isResult();
		return result;
	}

	@Override
	public Throwable getException() {
		assert isException();
		return exception;
	}

	@Override
	public Try<T> getTry() {
		return Try.of(result, exception);
	}

	@Override
	public boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		consumer.accept(result, exception);
		return true;
	}

	@Override
	public boolean setResultTo(Consumer<? super T> consumer) {
		if (isResult()) {
			consumer.accept(result);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean setExceptionTo(Consumer<Throwable> consumer) {
		if (isException()) {
			consumer.accept(exception);
			return true;
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public <U> CompleteStage<U> mold() {
		assert isException() : "Trying to mold a successful CompleteStage!";
		return (CompleteStage<U>) this;
	}

	@Override
	public <U, S extends BiConsumer<? super T, Throwable> & Stage<U>> Stage<U> then(S stage) {
		stage.accept(result, exception);
		return stage;
	}

	@Override
	public <U> Stage<U> thenApply(Function<? super T, ? extends U> fn) {
		return isResult() ? Stage.of(fn.apply(result)) : mold();
	}

	@Override
	public <U> Stage<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		return Stage.of(fn.apply(result, exception));
	}

	@Override
	public Stage<T> thenRun(Runnable action) {
		if (isResult()) action.run();
		return this;
	}

	@Override
	public Stage<T> thenRunEx(Runnable action) {
		action.run();
		return this;
	}

	@Override
	public <U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn) {
		return isResult() ? fn.apply(result) : mold();
	}

	@Override
	public <U> Stage<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Stage<U>> fn) {
		return fn.apply(result, exception);
	}

	@Override
	public Stage<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		action.accept(result, exception);
		return this;
	}

	@Override
	public Stage<T> whenResult(Consumer<? super T> action) {
		if (isResult()) action.accept(result);
		return this;
	}

	@Override
	public Stage<T> whenException(Consumer<Throwable> action) {
		if (isException()) action.accept(exception);
		return this;
	}

	@Override
	public Stage<T> thenException(Function<? super T, Throwable> fn) {
		return isResult() ? Stage.ofException(fn.apply(result)) : mold();
	}

	@Override
	public <U> Stage<U> thenTry(ThrowingFunction<? super T, ? extends U> fn) {
		if (isException()) return mold();
		try {
			return Stage.of(fn.apply(result));
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			return Stage.ofException(e);
		}
	}

	@Override
	public <U, V> Stage<V> combine(Stage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		if (isException()) return mold();
		if (other instanceof CompleteStage) {
			@SuppressWarnings("unchecked") CompleteStage<U> otherComplete = (CompleteStage<U>) other;
			if (otherComplete.isException()) return otherComplete.mold();
			return Stage.of(fn.apply(this.result, otherComplete.getResult()));
		}
		return other.then(new NextStage<U, V>() {
			@Override
			protected void onComplete(U result) {
				complete(fn.apply(CompleteStage.this.result, result));
			}
		});
	}

	@Override
	public Stage<Void> both(Stage<?> other) {
		if (isException()) return mold();
		if (other instanceof CompleteStage) {
			CompleteStage<?> otherComplete = (CompleteStage<?>) other;
			if (otherComplete.isException()) return otherComplete.mold();
			return Stage.complete();
		}
		return other.toVoid();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<T> either(Stage<? extends T> other) {
		return isResult() ? this : (Stage<T>) other;
	}

	@Override
	public MaterializedStage<T> async() {
		SettableStage<T> result = new SettableStage<>();
		getCurrentEventloop().post(isResult() ?
				() -> result.set(this.result) :
				() -> result.setException(exception));
		return result;
	}

	@Override
	public Stage<Try<T>> toTry() {
		return isResult() ? Stage.of(Try.of(result)) : mold();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<Void> toVoid() {
		return isResult() ? Stage.complete() : mold();
	}

	@Override
	public Stage<T> timeout(@Nullable Duration timeout) {
		return this;
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		if (isResult()) {
			future.complete(result);
		} else {
			future.completeExceptionally(exception);
		}
		return future;
	}
}
