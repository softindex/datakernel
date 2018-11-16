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

package io.datakernel.csp;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.dsl.ChannelSupplierTransformer;
import io.datakernel.csp.queue.ChannelQueue;
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
public interface ChannelSupplier<T> extends Cancellable {
	Promise<T> get();

	static <T> ChannelSupplier<T> ofConsumer(Consumer<ChannelConsumer<T>> consumer, ChannelQueue<T> queue) {
		consumer.accept(queue.getConsumer());
		return queue.getSupplier();
	}

	static <T> ChannelSupplier<T> ofSupplier(Supplier<? extends Promise<T>> supplier) {
		return of(AsyncSupplier.of(supplier));
	}

	static <T> ChannelSupplier<T> of(AsyncSupplier<T> supplier) {
		return of(supplier, null);
	}

	static <T> ChannelSupplier<T> of(AsyncSupplier<T> supplier, @Nullable Cancellable cancellable) {
		return new AbstractChannelSupplier<T>(cancellable) {
			@Override
			protected Promise<T> doGet() {
				return supplier.get();
			}
		};
	}

	static <T> ChannelSupplier<T> of() {
		return new AbstractChannelSupplier<T>() {
			@Override
			protected Promise<T> doGet() {
				return Promise.of(null);
			}
		};
	}

	static <T> ChannelSupplier<T> of(T value) {
		return ChannelSuppliers.of(value);
	}

	@SafeVarargs
	static <T> ChannelSupplier<T> of(T... values) {
		return ofIterator(asIterator(values));
	}

	static <T> ChannelSupplier<T> ofException(Throwable e) {
		return new AbstractChannelSupplier<T>() {
			@Override
			protected Promise<T> doGet() {
				return Promise.ofException(e);
			}
		};
	}

	static <T> ChannelSupplier<T> ofIterable(Iterable<? extends T> iterable) {
		return ofIterator(iterable.iterator());
	}

	static <T> ChannelSupplier<T> ofStream(Stream<? extends T> stream) {
		return ofIterator(stream.iterator());
	}

	static <T> ChannelSupplier<T> ofIterator(Iterator<? extends T> iterator) {
		return new AbstractChannelSupplier<T>() {
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
	 * Wraps {@link AsyncTcpSocket#read()} operation into {@link ChannelSupplier}
	 *
	 * @return {@link ChannelSupplier} of ByteBufs that are read from network
	 */
	static ChannelSupplier<ByteBuf> ofSocket(AsyncTcpSocket socket) {
		return ChannelSuppliers.prefetch(ChannelSupplier.of(socket::read, socket));
	}

	static <T> ChannelSupplier<T> ofPromise(Promise<? extends ChannelSupplier<T>> promise) {
		if (promise.isResult()) return promise.materialize().getResult();
		MaterializedPromise<? extends ChannelSupplier<T>> materializedPromise = promise.materialize();
		return new AbstractChannelSupplier<T>() {
			ChannelSupplier<T> supplier;
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

	static <T> ChannelSupplier<T> ofLazyProvider(Supplier<? extends ChannelSupplier<T>> provider) {
		return new AbstractChannelSupplier<T>() {
			private ChannelSupplier<T> supplier;

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

	default <R> R transformWith(ChannelSupplierTransformer<T, R> fn) {
		return fn.transform(this);
	}

	default ChannelSupplier<T> async() {
		return new AbstractChannelSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return ChannelSupplier.this.get().async();
			}
		};
	}

	default ChannelSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		return new AbstractChannelSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return asyncExecutor.execute(ChannelSupplier.this::get);
			}
		};
	}

	default ChannelSupplier<T> peek(Consumer<? super T> fn) {
		return new AbstractChannelSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return ChannelSupplier.this.get()
						.whenResult(value -> { if (value != null) fn.accept(value);});
			}
		};
	}

	default <V> ChannelSupplier<V> map(Function<? super T, ? extends V> fn) {
		return new AbstractChannelSupplier<V>(this) {
			@Override
			protected Promise<V> doGet() {
				return ChannelSupplier.this.get()
						.thenApply(value -> {
							if (value != null) {
								try {
									return fn.apply(value);
								} catch (UncheckedException u) {
									ChannelSupplier.this.close(u.getCause());
									throw u;
								}
							} else {
								return null;
							}
						});
			}
		};
	}

	default <V> ChannelSupplier<V> mapAsync(Function<? super T, ? extends Promise<V>> fn) {
		return new AbstractChannelSupplier<V>(this) {
			@Override
			protected Promise<V> doGet() {
				return ChannelSupplier.this.get()
						.thenCompose(value -> value != null ?
								fn.apply(value) :
								Promise.of(null));
			}
		};
	}

	default ChannelSupplier<T> filter(Predicate<? super T> predicate) {
		return new AbstractChannelSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				while (true) {
					Promise<T> promise = ChannelSupplier.this.get();
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

	default MaterializedPromise<Void> streamTo(ChannelConsumer<T> consumer) {
		return ChannelSuppliers.streamTo(this, consumer);
	}

	default MaterializedPromise<Void> bindTo(ChannelInput<T> to) {
		return to.set(this);
	}

	default <A, R> Promise<R> toCollector(Collector<T, A, R> collector) {
		return ChannelSuppliers.toCollector(this, collector);
	}

	default Promise<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	default ChannelSupplier<T> withEndOfStream(Function<Promise<Void>, Promise<Void>> fn) {
		SettablePromise<Void> endOfStream = new SettablePromise<>();
		MaterializedPromise<Void> newEndOfStream = fn.apply(endOfStream).materialize();
		return new AbstractChannelSupplier<T>() {
			@SuppressWarnings("unchecked")
			@Override
			protected Promise<T> doGet() {
				return ChannelSupplier.this.get()
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
