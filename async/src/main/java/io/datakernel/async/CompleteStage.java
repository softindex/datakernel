package io.datakernel.async;

import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public abstract class CompleteStage<T> implements MaterializedStage<T> {
	@Override
	public final boolean isComplete() {
		return true;
	}

	@Override
	public final boolean isResult() {
		return true;
	}

	@Override
	public final boolean isException() {
		return false;
	}

	@Override
	public final boolean hasResult() {
		return true;
	}

	@Override
	public final boolean hasException() {
		return false;
	}

	@Override
	abstract public T getResult();

	@Override
	public final Throwable getException() {
		throw new UnsupportedOperationException();
	}

	@Override
	public final Try<T> asTry() {
		return Try.of(getResult());
	}

	@Override
	public final boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		consumer.accept(getResult(), null);
		return true;
	}

	@Override
	public final boolean setResultTo(Consumer<? super T> consumer) {
		consumer.accept(getResult());
		return true;
	}

	@Override
	public final boolean setExceptionTo(Consumer<Throwable> consumer) {
		return false;
	}

	@Override
	public final <U, S extends BiConsumer<? super T, Throwable> & Stage<U>> Stage<U> then(S stage) {
		stage.accept(getResult(), null);
		return stage;
	}

	@Override
	public final <U> Stage<U> thenApply(Function<? super T, ? extends U> fn) {
		try {
			return Stage.of(fn.apply(getResult()));
		} catch (UncheckedException u) {
			return Stage.ofException(u.getCause());
		}
	}

	@Override
	public final <U> Stage<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		try {
			return Stage.of(fn.apply(getResult(), null));
		} catch (UncheckedException u) {
			return Stage.ofException(u.getCause());
		}
	}

	@Override
	public final <U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn) {
		try {
			return fn.apply(getResult());
		} catch (UncheckedException u) {
			return Stage.ofException(u.getCause());
		}
	}

	@Override
	public final <U> Stage<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Stage<U>> fn) {
		try {
			return fn.apply(getResult(), null);
		} catch (UncheckedException u) {
			return Stage.ofException(u.getCause());
		}
	}

	@Override
	public final Stage<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		action.accept(getResult(), null);
		return this;
	}

	@Override
	public final Stage<T> whenResult(Consumer<? super T> action) {
		action.accept(getResult());
		return this;
	}

	@Override
	public final Stage<T> whenException(Consumer<Throwable> action) {
		return this;
	}

	@Override
	public final Stage<T> thenException(Function<? super T, Throwable> fn) {
		Throwable maybeException = fn.apply(getResult());
		if (maybeException == null) return this;
		return Stage.ofException(maybeException);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <U, V> Stage<V> combine(Stage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		if (other instanceof CompleteStage) {
			return Stage.of(fn.apply(getResult(), ((CompleteStage<U>) other).getResult()));
		}
		return other.thenApply(otherResult -> fn.apply(CompleteStage.this.getResult(), otherResult));
	}

	@Override
	public final Stage<Void> both(Stage<?> other) {
		if (other instanceof CompleteStage) {
			return Stage.complete();
		}
		return other.toVoid();
	}

	@SuppressWarnings("unchecked")
	@Override
	public final Stage<T> either(Stage<? extends T> other) {
		return this;
	}

	@Override
	public final MaterializedStage<T> async() {
		SettableStage<T> result = new SettableStage<>();
		getCurrentEventloop().post(() -> result.set(getResult()));
		return result;
	}

	@Override
	public final Stage<Try<T>> toTry() {
		return Stage.of(Try.of(getResult()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public final Stage<Void> toVoid() {
		return Stage.complete();
	}

	@Override
	public final CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.complete(getResult());
		return future;
	}
}
