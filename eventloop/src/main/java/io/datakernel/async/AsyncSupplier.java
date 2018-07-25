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

import java.util.function.*;

public interface AsyncSupplier<T> {
	Stage<T> get();

	default AsyncSupplier<T> with(UnaryOperator<AsyncSupplier<T>> modifier) {
		return modifier.apply(this);
	}

	static <T> AsyncSupplier<T> of(Supplier<? extends Stage<T>> supplier) {
		return supplier::get;
	}

	static <A, T> AsyncSupplier<T> of(AsyncFunction<? super A, T> function, A a) {
		return () -> function.apply(a);
	}

	static <A, B, T> AsyncSupplier<T> of(BiFunction<? super A, ? super B, Stage<T>> biFunction, A a, B b) {
		return () -> biFunction.apply(a, b);
	}

	default AsyncSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		return () -> asyncExecutor.execute(this);
	}

	default <V> AsyncSupplier<V> transform(Function<? super T, ? extends V> fn) {
		return () -> get().thenApply(fn);
	}

	@SuppressWarnings("unchecked")
	default <V> AsyncSupplier<V> transformAsync(Function<? super T, ? extends Stage<V>> fn) {
		return () -> get().thenCompose(fn::apply);
	}

	default AsyncSupplier<T> thenRun(Runnable action) {
		return () -> get().thenRun(action);
	}

	default AsyncSupplier<T> thenRunEx(Runnable action) {
		return () -> get().thenRunEx(action);
	}

	default AsyncSupplier<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		return () -> get().whenComplete(action);
	}

	default AsyncSupplier<T> whenResult(Consumer<? super T> action) {
		return () -> get().whenResult(action);
	}

	default AsyncSupplier<T> whenException(Consumer<Throwable> action) {
		return () -> get().whenException(action);
	}

}
