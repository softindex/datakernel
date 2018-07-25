/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.async;

import java.util.Objects;
import java.util.function.Function;

/**
 * Asynchronous determines an output value based on an input value.
 *
 * @param <T> type of input object
 * @param <R> type of output object
 */
public interface AsyncFunction<T, R> {
	/**
	 * Returns the result of applying this function to input.
	 */
	Stage<R> apply(T input);

	default <U> AsyncFunction<U, R> compose(Function<? super U, ? extends T> before) {
		return (U t) -> this.apply(before.apply(t));
	}

	default <U> AsyncFunction<U, R> compose(AsyncFunction<? super U, ? extends T> before) {
		return input -> before.apply(input).thenCompose(this::apply);
	}

	default <U> AsyncFunction<T, U> andThen(Function<? super R, ? extends U> after) {
		Objects.requireNonNull(after);
		return (T i) -> apply(i).thenApply(after);
	}

	@SuppressWarnings("unchecked")
	default <U> AsyncFunction<T, U> andThen(AsyncFunction<? super R, ? extends U> after) {
		Objects.requireNonNull(after);
		return (T i) -> apply(i).thenCompose((R input) -> (Stage<U>) after.apply(input));
	}

	default AsyncSupplier<R> asAsyncCallable(T argument) {
		return AsyncSupplier.of(this, argument);
	}

	static <T, R> AsyncFunction<T, R> of(Function<T, R> function) {
		return input -> Stage.of(function.apply(input));
	}

	static <T> AsyncFunction<T, T> identity() {
		return Stage::of;
	}

}
