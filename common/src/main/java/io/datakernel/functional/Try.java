package io.datakernel.functional;

import io.datakernel.annotation.Nullable;

import java.util.function.Function;

public final class Try<T> {
	@Nullable
	private final T result;
	@Nullable
	private final Throwable throwable;

	private Try(@Nullable T result, @Nullable Throwable throwable) {
		this.result = result;
		this.throwable = throwable;
	}

	public static <T> Try<T> of(@Nullable T result) {
		return new Try<>(result, null);
	}

	public static <T> Try<T> ofFailure(Throwable throwable) {
		return new Try<>(null, throwable);
	}

	public static <T> Try<T> wrap(ThrowingSupplier<T> computation) {
		try {
			return new Try<>(computation.get(), null);
		} catch (Throwable t) {
			return new Try<>(null, t);
		}
	}

	public boolean isSuccess() {
		return throwable == null;
	}

	@Nullable
	public T get() throws Throwable {
		if (throwable == null) {
			return result;
		}
		throw throwable;
	}

	@Nullable
	public T getOrNull() {
		if (throwable == null) {
			return result;
		}
		return null;
	}

	@Nullable
	public Throwable getThrowable() {
		return throwable;
	}

	@SuppressWarnings("unchecked")
	private <U> Try<U> mold() {
		assert throwable != null : "Trying to mold a successful Try!";
		return (Try<U>) this;
	}

	public <U> Try<U> map(ThrowingFunction<T, U> function) {
		if (throwable == null) {
			try {
				return new Try<>(function.apply(result), null);
			} catch (Throwable t) {
				return new Try<>(null, t);
			}
		}
		return mold();
	}

	public <U> Try<U> flatMap(Function<T, Try<U>> function) {
		if (throwable == null) {
			return function.apply(result);
		}
		return mold();
	}

	public Try<T> exceptionally(Function<Throwable, T> function) {
		if (throwable != null) {
			return new Try<>(function.apply(throwable), null);
		}
		return this;
	}

	public Either<Throwable, T> toEither() {
		if (throwable == null) {
			return Either.right(result);
		}
		return Either.left(throwable);
	}

	@FunctionalInterface
	public interface ThrowingSupplier<T> {
		@Nullable
		T get() throws Throwable;
	}

	@FunctionalInterface
	interface ThrowingFunction<T, R> {

		@Nullable
		R apply(@Nullable T arg) throws Throwable;
	}
}
