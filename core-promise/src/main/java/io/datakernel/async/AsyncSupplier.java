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

import io.datakernel.functional.Try;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.datakernel.async.Utils.forEachRemainingImpl;

/**
 * This interface represents asynchronous supplier that returns {@link Promise} of some data.
 */
@FunctionalInterface
public interface AsyncSupplier<T> {
	/**
	 * Asynchronous operation that is used to get {@link Promise} of data item.
	 *
	 * @return {@link Promise} of data item
	 */
	Promise<T> get();

	static <T> AsyncSupplier<T> of(Supplier<? extends Promise<T>> supplier) {
		return supplier::get;
	}

	@SuppressWarnings("unchecked")
	static <T> AsyncSupplier<T> cast(AsyncSupplier<? extends T> supplier) {
		return (AsyncSupplier<T>) supplier;
	}

	static <T> AsyncSupplier<T> ofValue(T value) {
		return () -> Promise.of(value);
	}

	static <T> AsyncSupplier<T> ofSupplier(Supplier<? extends T> supplier) {
		return () -> Promise.of(supplier.get());
	}

	static <T> AsyncSupplier<T> ofIterator(Iterator<? extends T> iterator) {
		return () -> Promise.of(iterator.hasNext() ? iterator.next() : null);
	}

	static <T> AsyncSupplier<T> ofStream(Stream<? extends T> stream) {
		return ofIterator(stream.iterator());
	}

	static <T> AsyncSupplier<T> ofIterable(Iterable<? extends T> iterable) {
		return ofIterator(iterable.iterator());
	}

	static <T> AsyncSupplier<T> ofPromise(Promise<T> promise) {
		return () -> promise;
	}

	static <T> AsyncSupplier<T> ofPromiseIterator(Iterator<? extends Promise<T>> iterator) {
		return () -> iterator.hasNext() ? iterator.next() : Promise.of(null);
	}

	static <T> AsyncSupplier<T> ofPromiseIterable(Iterable<? extends Promise<T>> iterable) {
		return ofPromiseIterator(iterable.iterator());
	}

	static <T> AsyncSupplier<T> ofPromiseStream(Stream<? extends Promise<T>> stream) {
		return ofPromiseIterator(stream.iterator());
	}

	static <T> AsyncSupplier<T> ofAsyncSupplierIterator(Iterator<? extends AsyncSupplier<T>> iterator) {
		return () -> iterator.hasNext() ? iterator.next().get() : Promise.of(null);
	}

	static <T> AsyncSupplier<T> ofAsyncSupplierIterable(Iterable<? extends AsyncSupplier<T>> iterable) {
		return ofAsyncSupplierIterator(iterable.iterator());
	}

	static <T> AsyncSupplier<T> ofAsyncSupplierStream(Stream<? extends AsyncSupplier<T>> stream) {
		return ofAsyncSupplierIterator(stream.iterator());
	}

	default <R> R transformWith(Function<AsyncSupplier<T>, R> fn) {
		return fn.apply(this);
	}

	/**
	 * Method to ensure that supplied promise will complete asynchronously.
	 *
	 * @return {@link AsyncSupplier} of promises that will be completed asynchronously
	 * @see Promise#async()
	 */
	default AsyncSupplier<T> async() {
		return () -> get().async();
	}

	default AsyncSupplier<Void> toVoid() {
		return () -> get().toVoid();
	}

	default AsyncSupplier<Try<T>> toTry() {
		return () -> get().toTry();
	}

	default AsyncSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		return () -> asyncExecutor.execute(this);
	}

	default Promise<Void> forEachRemaining(Consumer<T> consumer) {
		return forEachRemainingAsync(AsyncConsumer.of(consumer));
	}

	default Promise<Void> forEachRemainingAsync(AsyncConsumer<T> consumer) {
		return Promise.ofCallback(cb -> forEachRemainingImpl(this, consumer, cb));
	}

	/**
	 * Applies function before supplying a promise.
	 *
	 * @param fn function to be applied to result of promise
	 * @return {@link AsyncSupplier} of promises after transformation
	 */
	default <V> AsyncSupplier<V> map(Function<? super T, ? extends V> fn) {
		return () -> get().thenApply(fn);
	}

	/**
	 * Applies function to the result of supplied promise.
	 *
	 * @param fn  - function to be applied to result of promise
	 * @param <V>
	 * @return
	 */
	default <V> AsyncSupplier<V> mapAsync(Function<? super T, ? extends Promise<V>> fn) {
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
