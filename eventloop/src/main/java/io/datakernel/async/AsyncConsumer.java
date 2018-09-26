package io.datakernel.async;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * This interface represents asynchronous consumer that consumes data items.
 */
@FunctionalInterface
public interface AsyncConsumer<T> {
	/**
	 * Asynchronous operation to consume some data
	 *
	 * @param value value to be consumed
	 * @return {@link Stage} of {@link Void} that represents succesful consumption of data
	 */
	Stage<Void> accept(T value);

	/**
	 * Wrapper around standard Java's {@link Consumer} interface.
	 *
	 * @param consumer - Java's {@link Consumer} of Stages
	 * @return {@link AsyncSupplier} that works on top of standard Java's {@link Supplier} interface
	 */

	static <T> AsyncConsumer<T> of(Consumer<? super T> consumer) {
		return value -> {
			consumer.accept(value);
			return Stage.complete();
		};
	}

	default AsyncConsumer<T> with(UnaryOperator<AsyncConsumer<T>> modifier) {
		return modifier.apply(this);
	}

	default AsyncConsumer<T> async() {
		return value -> accept(value).async();
	}

	default AsyncConsumer<T> withExecutor(AsyncExecutor asyncExecutor) {
		return value -> asyncExecutor.execute(() -> accept(value));
	}

	default <V> AsyncConsumer<V> transform(Function<? super V, ? extends T> fn) {
		return value -> accept(fn.apply(value));
	}

	default <V> AsyncConsumer<V> transformAsync(Function<? super V, ? extends Stage<T>> fn) {
		return value -> fn.apply(value).thenCompose(this::accept);
	}

	default AsyncConsumer<T> thenRun(Runnable action) {
		return value -> accept(value).thenRun(action);
	}

	default AsyncConsumer<T> thenRunEx(Runnable action) {
		return value -> accept(value).thenRunEx(action);
	}

	default AsyncConsumer<T> whenException(Consumer<Throwable> action) {
		return value -> accept(value).whenException(action);
	}
}
