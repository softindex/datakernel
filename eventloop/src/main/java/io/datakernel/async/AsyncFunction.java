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
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Asynchronous determines an output value based on an input value.
 *
 * @param <I> type of input object
 * @param <O> type of output object
 */
public interface AsyncFunction<I, O> {
	/**
	 * Returns the result of applying this function to input.
	 */
	CompletionStage<O> apply(I input);

	default <T> AsyncFunction<T, O> compose(Function<? super T, ? extends I> before) {
		return (T t) -> this.apply(before.apply(t));
	}

	default <T> AsyncFunction<T, O> compose(AsyncFunction<? super T, ? extends I> before) {
		return input -> before.apply(input).thenCompose(this::apply);
	}

	default <T> AsyncFunction<I, T> andThen(Function<? super O, ? extends T> after) {
		Objects.requireNonNull(after);
		return (I i) -> apply(i).thenApply(after);
	}

	default <T> AsyncFunction<I, T> andThen(AsyncFunction<? super O, ? extends T> after) {
		Objects.requireNonNull(after);
		return (I i) -> apply(i).thenCompose((O input) -> (CompletionStage<T>) after.apply(input));
	}

	static <I, O> AsyncFunction<I, O> of(Function<I, O> function) {
		return input -> Stages.of(function.apply(input));
	}

	static <T> AsyncFunction<T, T> identity() {
		return Stages::of;
	}

}
