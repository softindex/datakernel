package io.datakernel.functional;

import io.datakernel.annotation.Nullable;
import io.datakernel.util.ThrowingRunnable;
import io.datakernel.util.ThrowingSupplier;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.datakernel.util.Preconditions.checkState;

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

	public static <T> Try<T> of(@Nullable T result, @Nullable Throwable throwable) {
		assert !(result != null && throwable != null);
		return new Try<>(result, throwable);
	}

	public static <T> Try<T> ofException(Throwable throwable) {
		assert throwable != null;
		return new Try<>(null, throwable);
	}

	public static <T> Try<T> wrap(ThrowingSupplier<T> computation) {
		try {
			return new Try<>(computation.get(), null);
		} catch (Throwable t) {
			return new Try<>(null, t);
		}
	}

	public static <T> Try<T> wrap(ThrowingRunnable computation) {
		try {
			computation.run();
			return new Try<>(null, null);
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
	public T getOr(@Nullable T defaultValue) {
		if (throwable == null) {
			return result;
		}
		return defaultValue;
	}

	@Nullable
	public T getOrSupply(Supplier<? extends T> defaultValueSupplier) {
		if (throwable == null) {
			return result;
		}
		return defaultValueSupplier.get();
	}

	@Nullable
	public T getOrNull() {
		return result;
	}

	@Nullable
	public Throwable getExceptionOrNull() {
		return throwable;
	}

	@SuppressWarnings("unchecked")
	private <U> Try<U> mold() {
		checkState(throwable != null, "Trying to mold a successful Try!");
		return (Try<U>) this;
	}

	public <U> U reduce(Function<? super T, ? extends U> function, Function<Throwable, ? extends U> exceptionFunction) {
		if (throwable == null) {
			return function.apply(result);
		}
		return exceptionFunction.apply(throwable);
	}

	public <U> U reduce(BiFunction<? super T, Throwable, ? extends U> fn) {
		return fn.apply(result, throwable);
	}

	public <U> Try<U> map(Function<T, U> function) {
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
}
