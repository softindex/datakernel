/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.functional;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.UncheckedException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.*;
import java.util.stream.Collector;

import static io.datakernel.util.Preconditions.checkState;

public final class Try<T> {
	private final T result;
	@Nullable
	private final Throwable throwable;

	private Try(T result, @Nullable Throwable e) {
		this.result = result;
		this.throwable = e;
	}

	public static <T> Try<T> of(T result) {
		return new Try<>(result, null);
	}

	public static <T> Try<T> of(T result, @Nullable Throwable e) {
		assert result == null || e == null;
		return new Try<>(result, e);
	}

	public static <T> Try<T> ofException(Throwable e) {
		assert e != null;
		return new Try<>(null, e);
	}

	public static <T> Try<T> wrap(Supplier<T> computation) {
		try {
			return new Try<>(computation.get(), null);
		} catch (UncheckedException u) {
			return new Try<>(null, u.getCause());
		}
	}

	public static <T> Try<T> wrap(Runnable computation) {
		try {
			computation.run();
			return new Try<>(null, null);
		} catch (UncheckedException u) {
			return new Try<>(null, u.getCause());
		}
	}

	public static Collector<Try<Void>, ?, Try<Void>> voidReducer() {
		return reducer(($1, $2) -> null);
	}

	public static <T> Collector<Try<T>, ?, Try<T>> reducer(BinaryOperator<T> combiner) {
		return reducer(null, combiner);
	}

	public static <T> Collector<Try<T>, ?, Try<T>> reducer(@Nullable T identity, BinaryOperator<T> combiner) {
		class Accumulator {
			T result = identity;
			List<Throwable> throwables = new ArrayList<>();
		}
		return Collector.of(Accumulator::new, (acc, t) -> {
			if (t.isSuccess()) {
				acc.result = acc.result != null ? combiner.apply(acc.result, t.getResult()) : t.getResult();
			} else {
				acc.throwables.add(t.getException());
			}
		}, (acc1, acc2) -> {
			acc1.result = combiner.apply(acc1.result, acc2.result);
			acc1.throwables.addAll(acc2.throwables);
			return acc1;
		}, acc -> {
			if (acc.throwables.isEmpty()) {
				return Try.of(acc.result);
			}
			Throwable e = acc.throwables.get(0);
			for (Throwable t : acc.throwables) {
				if (t != e) {
					e.addSuppressed(t);
				}
			}
			return Try.ofException(e);
		});
	}

	public boolean isSuccess() {
		return throwable == null;
	}

	public T get() throws Exception {
		if (throwable == null) {
			return result;
		}
		throw throwable instanceof Exception ? (Exception) throwable : new RuntimeException(throwable);
	}

	public T getOr(T defaultValue) {
		if (throwable == null) {
			return result;
		}
		return defaultValue;
	}

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

	public T getResult() {
		assert isSuccess();
		return result;
	}

	public Throwable getException() {
		assert !isSuccess();
		return throwable;
	}

	@Nullable
	public Throwable getExceptionOrNull() {
		return throwable;
	}

	public void setTo(BiConsumer<? super T, Throwable> consumer) {
		consumer.accept(result, throwable);
	}

	public boolean setResultTo(Consumer<? super T> consumer) {
		if (isSuccess()) {
			consumer.accept(result);
			return true;
		}
		return false;
	}

	public boolean setExceptionTo(Consumer<Throwable> consumer) {
		if (!isSuccess()) {
			consumer.accept(throwable);
			return true;
		}
		return false;
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
			} catch (Throwable e) {
				return new Try<>(null, e);
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Try<?> other = (Try<?>) o;
		if (result != null ? !result.equals(other.result) : other.result != null) return false;
		return throwable != null ? throwable.equals(other.throwable) : other.throwable == null;
	}

	@Override
	public int hashCode() {
		int hash = result != null ? result.hashCode() : 0;
		hash = 31 * hash + (throwable != null ? throwable.hashCode() : 0);
		return hash;
	}

	@Override
	public String toString() {
		return "{" + (isSuccess() ? "" + result : "" + throwable) + "}";
	}
}
