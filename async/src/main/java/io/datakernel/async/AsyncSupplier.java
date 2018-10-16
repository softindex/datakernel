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

package io.datakernel.async;

import java.util.function.*;

/**
 * This interface represents asynchronous supplier that returns {@link Stage} of some data.
 *
 */
@FunctionalInterface
public interface AsyncSupplier<T> {
	/**
	 * Asynchronous operation that is used to get {@link Stage} of data item.
	 *
	 * @return {@link Stage} of data item
	 */
	Stage<T> get();

	/**
	 * Wrapper around standard Java's {@link Supplier} interface.
	 *
	 * @param supplier - Java's {@link Supplier} of Stages
	 * @return {@link AsyncSupplier} that works on top of standard Java's {@link Supplier} interface
	 */
	static <T> AsyncSupplier<T> of(Supplier<? extends Stage<T>> supplier) {
		return supplier::get;
	}

	default AsyncSupplier<T> with(UnaryOperator<AsyncSupplier<T>> modifier) {
		return modifier.apply(this);
	}

	/**
	 * Method to ensure that supplied stage will complete asynchronously.
	 *
	 * @see Stage#async()
	 * @return {@link AsyncSupplier} of stages that will be completed asynchronously
	 */
	default AsyncSupplier<T> async() {
		return () -> get().async();
	}

	default AsyncSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		return () -> asyncExecutor.execute(this);
	}

	/**
	 * Applies function before supplying a stage.
	 *
	 * @param fn function to be applied to result of stage
	 * @return {@link AsyncSupplier} of stages after transformation
	 */
	default <V> AsyncSupplier<V> transform(Function<? super T, ? extends V> fn) {
		return () -> get().thenApply(fn);
	}

	/**
	 * Applies function to the result of supplied stage.
	 *
	 * @param fn - function to be applied to result of stage
	 * @param <V>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	default <V> AsyncSupplier<V> transformAsync(Function<? super T, ? extends Stage<V>> fn) {
		return () -> get().thenCompose(fn::apply);
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
