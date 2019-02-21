/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.async.*;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettableCallback;
import io.datakernel.csp.queue.ChannelBuffer;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.StacklessException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;
import io.datakernel.util.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.datakernel.util.Recyclable.deepRecycle;

/**
 * Provides additional functionality for managing {@link ChannelSupplier}s.
 * Includes helper classes: ChannelSupplierOfException, ChannelSupplierOfIterator,
 * ChannelSupplierOfValue, ChannelSupplierEmpty.
 */
public final class ChannelSuppliers {
	private ChannelSuppliers() {
	}

	/**
	 * @see #concat(Iterator)
	 */
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T> supplier1, ChannelSupplier<? extends T> supplier2) {
		return concat(CollectionUtils.asIterator(supplier1, supplier2));
	}

	/**
	 * @see #concat(Iterator)
	 */
	@SafeVarargs
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T>... suppliers) {
		return concat(CollectionUtils.asIterator(suppliers));
	}

	/**
	 * Creates a new ChannelSupplier which on {@code get()} call returns
	 * the result wrapped in {@code promise} of the first ChannelSuppliers'
	 * {@code promise} that was successfully completed with a non-null result.
	 * If all of the ChannelSuppliers of the iterator have a {@code null}
	 * {@code promise} result, a {@code promise} of {@code null} will be returned.
	 * <p>
	 * If one of the ChannelSuppliers' {@code promises} completes with an exception,
	 * all subsequent elements of the iterator will be closed and a
	 * {@code promise} of exception will be returned.
	 *
	 * @param iterator an iterator of ChannelSuppliers
	 * @param <T>      type of data wrapped in the ChannelSuppliers
	 * @return a ChannelSupplier of {@code <T>}
	 */
	public static <T> ChannelSupplier<T> concat(Iterator<? extends ChannelSupplier<? extends T>> iterator) {
		return new AbstractChannelSupplier<T>() {
			ChannelSupplier<? extends T> current = ChannelSupplier.of();

			@Override
			protected Promise<T> doGet() {
				return current.get()
						.thenComposeEx((value, e) -> {
							if (e == null) {
								if (value != null) {
									return Promise.of(value);
								} else {
									if (iterator.hasNext()) {
										current = iterator.next();
										return get();
									} else {
										return Promise.of(null);
									}
								}
							} else {
								while (iterator.hasNext()) {
									iterator.next().close(e);
								}
								return Promise.ofException(e);
							}
						});
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				current.close(e);
				while (iterator.hasNext()) {
					iterator.next().close(e);
				}
			}
		};
	}

	/**
	 * Collects data provided by the {@code supplier} asynchronously and returns a
	 * promise of accumulated result. This process will be getting values from the
	 * {@code supplier}, until a promise of {@code null} is returned, which represents
	 * end of stream.
	 * <p>
	 * If {@code get} returns a promise of exception or there was an exception while
	 * {@code accumulator} accepted values, a promise of {@code exception} will be
	 * returned and the process will stop.
	 *
	 * @param supplier     a {@code ChannelSupplier} which provides data to be collected
	 * @param initialValue a value which will accumulate the results of accumulator
	 * @param accumulator  a {@link BiConsumer} which may perform some operations over provided
	 *                     by supplier data and accumulates the result to the initialValue
	 * @param finisher     a {@link Function} which performs the final transformation of the
	 *                     accumulated value
	 * @param <T>          a data type provided by the {@code supplier}
	 * @param <A>          an intermediate accumulation data type
	 * @param <R>          a data type of final result of {@code finisher}
	 * @return a promise of accumulated result, transformed by the {@code finisher}
	 */
	public static <T, A, R> Promise<R> collect(ChannelSupplier<T> supplier,
			A initialValue, BiConsumer<A, T> accumulator, Function<A, R> finisher) {
		return Promise.ofCallback(cb ->
				toCollectorImpl(supplier, initialValue, accumulator, finisher, cb));
	}

	private static <T, A, R> void toCollectorImpl(ChannelSupplier<T> supplier,
			A accumulatedValue, BiConsumer<A, T> accumulator, Function<A, R> finisher,
			SettableCallback<R> cb) {
		Promise<T> promise;
		while (true) {
			promise = supplier.get();
			if (!promise.isResult()) break;
			T item = promise.materialize().getResult();
			if (item != null) {
				try {
					accumulator.accept(accumulatedValue, item);
				} catch (UncheckedException u) {
					Throwable cause = u.getCause();
					supplier.close(cause);
					cb.setException(cause);
					return;
				}
				continue;
			}
			break;
		}
		promise.whenComplete((value, e) -> {
			if (e == null) {
				if (value != null) {
					try {
						accumulator.accept(accumulatedValue, value);
					} catch (UncheckedException u) {
						Throwable cause = u.getCause();
						supplier.close(cause);
						cb.setException(cause);
						return;
					}
					toCollectorImpl(supplier, accumulatedValue, accumulator, finisher, cb);
				} else {
					cb.set(finisher.apply(accumulatedValue));
				}
			} else {
				deepRecycle(finisher.apply(accumulatedValue));
				cb.setException(e);
			}
		});
	}

	public static <T> Promise<Void> streamTo(Promise<ChannelSupplier<T>> supplier, Promise<ChannelConsumer<T>> consumer) {
		return Promises.toTuple(supplier.toTry(), consumer.toTry())
				.thenCompose(t -> streamTo(t.getValue1(), t.getValue2()));
	}

	public static <T> MaterializedPromise<Void> streamTo(Try<ChannelSupplier<T>> supplier, Try<ChannelConsumer<T>> consumer) {
		if (supplier.isSuccess() && consumer.isSuccess()) {
			return streamTo(supplier.get(), consumer.get());
		}
		StacklessException exception = new StacklessException("Channel stream failed");
		supplier.consume(Cancellable::cancel, exception::addSuppressed);
		consumer.consume(Cancellable::cancel, exception::addSuppressed);
		return Promise.ofException(exception);
	}

	/**
	 * Streams data from the {@code supplier} to the {@code consumer} until {@code get()}
	 * of {@code supplier} returns a promise of {@code null}.
	 * <p>
	 * If {@code get} returns a promise of exception or there was an exception while
	 * {@code consumer} accepted values, a promise of {@code exception} will be
	 * returned and the process will stop.
	 *
	 * @param supplier a supplier which provides some data
	 * @param consumer a consumer which accepts the provided by supplier data
	 * @param <T>      a data type of values passed from the supplier to consumer
	 * @return a promise of {@code null} as a marker of completion of stream,
	 * or promise of exception, if there was an exception while streaming
	 */
	public static <T> MaterializedPromise<Void> streamTo(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer) {
		return Promise.ofCallback(cb ->
				streamToImpl(supplier, consumer, cb));
	}

	private static <T> void streamToImpl(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer, SettableCallback<Void> cb) {
		Promise<T> supplierPromise;
		while (true) {
			supplierPromise = supplier.get();
			if (!supplierPromise.isResult()) break;
			T item = supplierPromise.materialize().getResult();
			if (item == null) break;
			Promise<Void> consumerPromise = consumer.accept(item);
			if (consumerPromise.isResult()) continue;
			consumerPromise.whenComplete(($, e) -> {
				if (e == null) {
					streamToImpl(supplier, consumer, cb);
				} else {
					supplier.close(e);
					cb.trySetException(e);
				}
			});
			return;
		}
		supplierPromise
				.whenComplete((item, e1) -> {
					if (e1 == null) {
						consumer.accept(item)
								.whenComplete(($, e2) -> {
									if (e2 == null) {
										if (item != null) {
											streamToImpl(supplier, consumer, cb);
										} else {
											cb.trySet(null);
										}
									} else {
										supplier.close(e2);
										cb.trySetException(e2);
									}
								});
					} else {
						consumer.close(e1);
						cb.trySetException(e1);
					}
				});
	}

	@SuppressWarnings("unchecked")
	public static <T> ChannelSupplier<T> prefetch(int count, ChannelSupplier<T> actual) {
		ChannelBuffer<T> buffer = new ChannelBuffer<>(count);
		actual.streamTo(buffer.getConsumer());
		return buffer.getSupplier();
	}

	public static <T> ChannelSupplier<T> prefetch(ChannelSupplier<T> actual) {
		ChannelZeroBuffer<T> buffer = new ChannelZeroBuffer<>();
		actual.streamTo(buffer.getConsumer());
		return buffer.getSupplier();
	}

	/**
	 * Transforms this {@code ChannelSupplier} data of <T> type with provided {@code fn},
	 * which returns an {@link Iterator} of a <V> type. Then provides this value to ChannelSupplier of <V>.
	 */
	public static <T, V> ChannelSupplier<V> remap(ChannelSupplier<T> supplier, Function<? super T, ? extends Iterator<? extends V>> fn) {
		return new AbstractChannelSupplier<V>(supplier) {
			Iterator<? extends V> iterator = CollectionUtils.emptyIterator();
			boolean endOfStream;

			@Override
			protected Promise<V> doGet() {
				if (iterator.hasNext()) return Promise.of(iterator.next());
				return Promise.ofCallback(this::next);
			}

			private void next(SettableCallback<V> cb) {
				if (!endOfStream) {
					supplier.get()
							.whenComplete((item, e) -> {
								if (e == null) {
									if (item == null) endOfStream = true;
									iterator = fn.apply(item);
									if (iterator.hasNext()) {
										cb.set(iterator.next());
									} else {
										next(cb);
									}
								} else {
									cb.setException(e);
								}
							});
				} else {
					cb.set(null);
				}
			}
		};
	}

	/**
	 * Represents a {@code ChannelSupplier} which always returns
	 * a promise of {@code null}.
	 */
	public static class ChannelSupplierEmpty<T> extends AbstractChannelSupplier<T> {
		@Override
		protected Promise<T> doGet() {
			return Promise.of(null);
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} of one value. Returns a promise of the value when
	 * {@code get} is called for the first time, all subsequent calls will return {@code null}.
	 */
	public static final class ChannelSupplierOfValue<T> extends AbstractChannelSupplier<T> {
		private T item;

		public T getValue() {
			return item;
		}

		public T takeValue() {
			T item = this.item;
			this.item = null;
			return item;
		}

		public ChannelSupplierOfValue(@NotNull T item) {
			this.item = item;
		}

		@Override
		protected Promise<T> doGet() {
			T item = takeValue();
			return Promise.of(item);
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} which wraps the provided iterator and
	 * returns promises of iterator's values until {@code hasNext()} is true, when
	 * there are no more values left, a promise of {@code null} is returned.
	 */
	public static final class ChannelSupplierOfIterator<T> extends AbstractChannelSupplier<T> {
		private final Iterator<? extends T> iterator;

		public ChannelSupplierOfIterator(Iterator<? extends T> iterator) {
			this.iterator = iterator;
		}

		@Override
		protected Promise<T> doGet() {
			return Promise.of(iterator.hasNext() ? iterator.next() : null);
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			deepRecycle(iterator);
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} which always returns a promise of exception.
	 */
	public static final class ChannelSupplierOfException<T> extends AbstractChannelSupplier<T> {
		private final Throwable e;

		public ChannelSupplierOfException(Throwable e) {
			this.e = e;
		}

		@Override
		protected Promise<T> doGet() {
			return Promise.ofException(e);
		}
	}
}
