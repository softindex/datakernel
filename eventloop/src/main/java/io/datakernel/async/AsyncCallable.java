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

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public interface AsyncCallable<T> {
	CompletionStage<T> call();

	static <T> AsyncCallable<T> of(Supplier<CompletionStage<T>> supplier) {
		return supplier::get;
	}

	static <A, T> AsyncCallable<T> of(AsyncFunction<? super A, T> function, A a) {
		return () -> function.apply(a);
	}

	static <A, B, T> AsyncCallable<T> of(BiFunction<? super A, ? super B, CompletionStage<T>> biFunction, A a, B b) {
		return () -> biFunction.apply(a, b);
	}

	static <A, T> AsyncCallable<T> singleCallOf(AsyncCallable<T> actualCallable) {
		return new AsyncCallable<T>() {
			SettableStage<T> runningStage;

			@Override
			public CompletionStage<T> call() {
				if (runningStage != null)
					return runningStage;
				runningStage = SettableStage.create();
				runningStage.whenComplete((result, throwable) -> runningStage = null);
				actualCallable.call().whenComplete(runningStage::set);
				return runningStage;
			}
		};
	}

	default <V> AsyncCallable<V> thenApply(Function<? super T, ? extends V> function) {
		return () -> call().thenApply(function);
	}

	default AsyncCallable<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
		return () -> call().whenComplete(action);
	}

	default AsyncCallable<T> exceptionally(Function<Throwable, ? extends T> fn) {
		return () -> call().exceptionally(fn);
	}

	default <U> AsyncCallable<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
		return () -> call().handle(fn);
	}
}