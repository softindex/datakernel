package io.datakernel.async;

import java.util.Objects;
import java.util.function.Predicate;

public interface AsyncPredicate<T> {
	Stage<Boolean> test(T t);

	default AsyncPredicate<T> negate() {
		return t -> this.test(t).thenApply(b -> !b);
	}

	default AsyncPredicate<T> and(AsyncPredicate<? super T> other) {
		Objects.requireNonNull(other);
		return t -> this.test(t).thenCompose(b -> b ? other.test(t) : Stage.of(Boolean.FALSE));
	}

	default AsyncPredicate<T> and(Predicate<? super T> other) {
		Objects.requireNonNull(other);
		return t -> this.test(t).thenApply(b -> b ? other.test(t) : Boolean.FALSE);
	}

	default AsyncPredicate<T> or(Predicate<? super T> other) {
		Objects.requireNonNull(other);
		return t -> this.test(t).thenApply(b -> b ? Boolean.TRUE : other.test(t));
	}

	default AsyncPredicate<T> or(AsyncPredicate<? super T> other) {
		Objects.requireNonNull(other);
		return t -> this.test(t).thenCompose(b -> b ? Stage.of(Boolean.TRUE) : other.test(t));
	}

	static <T> AsyncPredicate<T> of(Predicate<T> predicate) {
		return t -> Stage.of(predicate.test(t));
	}

	static <T> AsyncPredicate<T> alwaysTrue() {
		return t -> Stage.of(Boolean.TRUE);
	}

	static <T> AsyncPredicate<T> alwaysFalse() {
		return t -> Stage.of(Boolean.FALSE);
	}

}
