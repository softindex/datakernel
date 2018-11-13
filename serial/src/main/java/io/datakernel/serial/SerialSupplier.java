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

package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.exception.UncheckedException;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.Recyclable.deepRecycle;
import static io.datakernel.util.Recyclable.tryRecycle;

/**
 * This interface represents supplier of {@link Promise} of data that should be used serially (each consecutive {@link #get()})
 * operation should be called only after previous {@link #get()} operation finishes.
 * <p>
 * After supplier is closed, all subsequent calls to {@link #get()} will return promise, completed exceptionally.
 * <p>
 * If any exception is caught while supplying data items, {@link #close(Throwable)} method should
 * be called. All resources should be freed and the caught exception should be propagated to all related processes.
 * <p>
 * If {@link #get()} returns {@link Promise} of {@code null}, it represents end-of-stream and means that no additional
 * data should be querried.
 */
public interface SerialSupplier<T> extends Cancellable {
	Promise<T> get();

	static <T> SerialSupplier<T> ofConsumer(Consumer<SerialConsumer<T>> consumer, SerialQueue<T> queue) {
		consumer.accept(queue.getConsumer());
		return queue.getSupplier();
	}

	static <T> SerialSupplier<T> ofSupplier(Supplier<? extends Promise<T>> supplier) {
		return of(AsyncSupplier.of(supplier));
	}

	static <T> SerialSupplier<T> of(AsyncSupplier<T> supplier) {
		return of(supplier, null);
	}

	static <T> SerialSupplier<T> of(AsyncSupplier<T> supplier, @Nullable Cancellable cancellable) {
		return new AbstractSerialSupplier<T>(cancellable) {
			@Override
			protected Promise<T> doGet() {
				return supplier.get();
			}
		};
	}

	static <T> SerialSupplier<T> of() {
		return new AbstractSerialSupplier<T>() {
			@Override
			protected Promise<T> doGet() {
				return Promise.of(null);
			}
		};
	}

	static <T> SerialSupplier<T> of(T value) {
		return SerialSuppliers.of(value);
	}

	@SafeVarargs
	static <T> SerialSupplier<T> of(T... values) {
		return ofIterator(asIterator(values));
	}

	static <T> SerialSupplier<T> ofException(Throwable e) {
		return new AbstractSerialSupplier<T>() {
			@Override
			protected Promise<T> doGet() {
				return Promise.ofException(e);
			}
		};
	}

	static <T> SerialSupplier<T> ofIterable(Iterable<? extends T> iterable) {
		return ofIterator(iterable.iterator());
	}

	static <T> SerialSupplier<T> ofStream(Stream<? extends T> stream) {
		return ofIterator(stream.iterator());
	}

	static <T> SerialSupplier<T> ofIterator(Iterator<? extends T> iterator) {
		return new AbstractSerialSupplier<T>() {
			@Override
			protected Promise<T> doGet() {
				return Promise.of(iterator.hasNext() ? iterator.next() : null);
			}

			@Override
			protected void onClosed(Throwable e) {
				deepRecycle(iterator);
			}
		};
	}

	/**
	 * Wraps {@link AsyncTcpSocket#read()} operation into {@link SerialSupplier}
	 *
	 * @return {@link SerialSupplier} of ByteBufs that are read from network
	 */
	static SerialSupplier<ByteBuf> ofSocket(AsyncTcpSocket socket) {
		return SerialSuppliers.prefetch(SerialSupplier.of(socket::read, socket));
	}

	static <T> SerialSupplier<T> ofPromise(Promise<? extends SerialSupplier<T>> promise) {
		if (promise.isResult()) return promise.materialize().getResult();
		MaterializedPromise<? extends SerialSupplier<T>> materializedPromise = promise.materialize();
		return new AbstractSerialSupplier<T>() {
			SerialSupplier<T> supplier;
			Throwable exception;

			@Override
			protected Promise<T> doGet() {
				if (supplier != null) return supplier.get();
				return materializedPromise.thenComposeEx((supplier, e) -> {
					if (e == null) {
						this.supplier = supplier;
						return supplier.get();
					} else {
						return Promise.ofException(e);
					}
				});
			}

			@Override
			protected void onClosed(Throwable e) {
				exception = e;
				materializedPromise.whenResult(supplier -> supplier.close(e));
			}
		};
	}

	static <T> SerialSupplier<T> ofLazyProvider(Supplier<? extends SerialSupplier<T>> provider) {
		return new AbstractSerialSupplier<T>() {
			private SerialSupplier<T> supplier;

			@Override
			protected Promise<T> doGet() {
				if (supplier == null) supplier = provider.get();
				return supplier.get();
			}

			@Override
			protected void onClosed(Throwable e) {
				if (supplier == null) supplier = provider.get();
				supplier.close(e);
			}
		};
	}

	default <R> R apply(SerialSupplierFunction<T, R> fn) {
		return fn.apply(this);
	}

	default SerialSupplier<T> async() {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return SerialSupplier.this.get().async();
			}
		};
	}

	default SerialSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		AsyncSupplier<T> supplier = this::get;
		return new AbstractSerialSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return asyncExecutor.execute(supplier);
			}
		};
	}

	default SerialSupplier<T> peek(Consumer<? super T> fn) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return SerialSupplier.this.get()
						.whenResult(value -> { if (value != null) fn.accept(value);});
			}
		};
	}

	default <V> SerialSupplier<V> transform(Function<? super T, ? extends V> fn) {
		return new AbstractSerialSupplier<V>(this) {
			@Override
			protected Promise<V> doGet() {
				return SerialSupplier.this.get()
						.thenApply(value -> {
							if (value != null) {
								try {
									return fn.apply(value);
								} catch (UncheckedException u) {
									SerialSupplier.this.close(u.getCause());
									throw u;
								}
							} else {
								return null;
							}
						});
			}
		};
	}

	default <V> SerialSupplier<V> transformAsync(Function<? super T, ? extends Promise<V>> fn) {
		return new AbstractSerialSupplier<V>(this) {
			@Override
			protected Promise<V> doGet() {
				return SerialSupplier.this.get()
						.thenCompose(value -> value != null ?
								fn.apply(value) :
								Promise.of(null));
			}
		};
	}

	default SerialSupplier<T> filter(Predicate<? super T> predicate) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				while (true) {
					Promise<T> promise = SerialSupplier.this.get();
					if (promise.isResult()) {
						T value = promise.materialize().getResult();
						if (value == null || predicate.test(value)) return promise;
						tryRecycle(value);
						continue;
					}
					return promise.thenCompose(value -> {
						if (value == null || predicate.test(value)) {
							return Promise.of(value);
						} else {
							tryRecycle(value);
							return get();
						}
					});
				}
			}
		};
	}

	default MaterializedPromise<Void> streamTo(SerialConsumer<T> consumer) {
		return SerialSuppliers.streamTo(this, consumer);
	}

	default MaterializedPromise<Void> bindTo(SerialInput<T> to) {
		return to.set(this);
	}

	default <A, R> Promise<R> toCollector(Collector<T, A, R> collector) {
		return SerialSuppliers.toCollector(this, collector);
	}

	default Promise<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	default SerialSupplier<T> withEndOfStream(Function<Promise<Void>, Promise<Void>> fn) {
		SettablePromise<Void> endOfStream = new SettablePromise<>();
		MaterializedPromise<Void> newEndOfStream = fn.apply(endOfStream).materialize();
		return new AbstractSerialSupplier<T>() {
			@SuppressWarnings("unchecked")
			@Override
			protected Promise<T> doGet() {
				return SerialSupplier.this.get()
						.thenComposeEx((item, e) -> {
							if (e == null) {
								if (item != null) return Promise.of(item);
								endOfStream.trySet(null);
								return (Promise<T>) newEndOfStream;
							} else {
								endOfStream.trySetException(e);
								return (Promise<T>) newEndOfStream;
							}
						});
			}

			@Override
			protected void onClosed(Throwable e) {
				endOfStream.trySetException(e);
			}
		};
	}

	static MaterializedPromise<Void> getEndOfStream(Consumer<Function<Promise<Void>, Promise<Void>>> cb) {
		SettablePromise<Void> result = new SettablePromise<>();
		cb.accept(endOfStream -> endOfStream.whenComplete(result::set));
		return result;
	}

}
