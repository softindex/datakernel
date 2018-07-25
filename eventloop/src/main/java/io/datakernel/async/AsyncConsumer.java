package io.datakernel.async;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public interface AsyncConsumer<T> {
	Stage<Void> accept(T value);

	default AsyncConsumer<T> with(UnaryOperator<AsyncConsumer<T>> modifier) {
		return modifier.apply(this);
	}

	static <T> AsyncConsumer<T> of(Consumer<? super T> consumer) {
		return value -> {
			consumer.accept(value);
			return Stage.of(null);
		};
	}

	default <V> AsyncConsumer<V> transform(Function<? super V, ? extends T> fn) {
		return value -> this.accept(fn.apply(value));
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
