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

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.exception.UncheckedException;
import io.datakernel.util.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.datakernel.util.Recyclable.deepRecycle;
import static io.datakernel.util.Recyclable.tryRecycle;

/**
 * Provides additional functionality for managing {@link ChannelSupplier}s.
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
	 * @param <T> type of data wrapped in the ChannelSuppliers
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

	public static <T, A, R> Promise<R> collect(ChannelSupplier<T> supplier,
			A initialValue, BiConsumer<A, T> accumulator, Function<A, R> finisher) {
		SettablePromise<R> cb = new SettablePromise<>();
		toCollectorImpl(supplier, initialValue, accumulator, finisher, cb);
		return cb;
	}

	private static <T, A, R> void toCollectorImpl(ChannelSupplier<T> supplier,
			A accumulatedValue, BiConsumer<A, T> accumulator, Function<A, R> finisher,
			SettablePromise<R> cb) {
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

	public static <T> MaterializedPromise<Void> streamTo(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer) {
		SettablePromise<Void> cb = new SettablePromise<>();
		streamToImpl(supplier, consumer, cb);
		return cb;
	}

	private static <T> void streamToImpl(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer, SettablePromise<Void> result) {
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
					streamToImpl(supplier, consumer, result);
				} else {
					supplier.close(e);
					result.trySetException(e);
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
											streamToImpl(supplier, consumer, result);
										} else {
											result.trySet(null);
										}
									} else {
										supplier.close(e2);
										result.trySetException(e2);
									}
								});
					} else {
						consumer.close(e1);
						result.trySetException(e1);
					}
				});
	}

	@SuppressWarnings("unchecked")
	public static <T> ChannelSupplier<T> prefetch(int count, ChannelSupplier<? extends T> actual) {
		return count == 0 ?
				(ChannelSupplier<T>) actual :
				new AbstractChannelSupplier<T>() {
					private final ArrayDeque<T> deque = new ArrayDeque<>();
					private boolean endOfStream;
					private boolean prefetching;
					@Nullable
					private SettablePromise<T> pending;

					{
						tryPrefetch();
					}

					private void tryPrefetch() {
						if (prefetching || deque.size() == count || endOfStream) return;
						prefetching = true;
						actual.get()
								.whenComplete((item, e) -> {
									if (isClosed()) return;
									prefetching = false;
									if (e == null) {
										assert pending == null || (deque.isEmpty() && !endOfStream);
										if (pending != null) {
											SettablePromise<T> pending = this.pending;
											this.pending = null;
											if (item != null) {
												tryPrefetch();
											} else {
												endOfStream = true;
											}
											pending.set(item);
											return;
										}
										if (item != null) {
											deque.add(item);
											tryPrefetch();
										} else {
											endOfStream = true;
										}
									} else {
										close(e);
									}
								});
					}

					@Override
					protected Promise<T> doGet() {
						assert pending == null;
						if (!deque.isEmpty() || endOfStream) {
							T result = deque.poll();
							tryPrefetch();
							return Promise.of(result);
						}
						SettablePromise<T> pending = new SettablePromise<>();
						this.pending = pending;
						tryPrefetch();
						return pending;
					}

					@Override
					protected void onClosed(@NotNull Throwable e) {
						deepRecycle(deque);
						actual.close(e);
						if (pending != null) {
							pending.trySetException(e);
							pending = null;
						}
					}
				};
	}

	public static <T> ChannelSupplier<T> prefetch(ChannelSupplier<? extends T> actual) {
		return new AbstractChannelSupplier<T>() {
			@Nullable
			private T prefetched;
			private boolean endOfStream;
			private boolean prefetching;
			@Nullable
			private SettablePromise<T> pending;

			{
				tryPrefetch();
			}

			private void tryPrefetch() {
				assert !isClosed();
				if (prefetching || prefetched != null || endOfStream) return;
				prefetching = true;
				actual.get()
						.whenComplete((item, e) -> {
							if (isClosed()) return;
							prefetching = false;
							if (e == null) {
								assert pending == null || (prefetched == null && !endOfStream);
								if (pending != null) {
									SettablePromise<T> pending = this.pending;
									this.pending = null;
									if (item != null) {
										// do nothing
									} else {
										endOfStream = true;
									}
									pending.set(item);
									return;
								}
								if (item != null) {
									prefetched = item;
								} else {
									endOfStream = true;
								}
							} else {
								close(e);
							}
						});
			}

			@Override
			protected Promise<T> doGet() {
				assert pending == null;
				if (prefetched != null || endOfStream) {
					T result = prefetched;
					prefetched = null;
					tryPrefetch();
					return Promise.of(result);
				}
				SettablePromise<T> pending = new SettablePromise<>();
				this.pending = pending;
				tryPrefetch();
				return pending;
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				tryRecycle(prefetched);
				prefetched = null;
				actual.close(e);
				if (pending != null) {
					pending.setException(e);
					pending = null;
				}
			}
		};
	}

	public static <T, V> ChannelSupplier<V> remap(ChannelSupplier<T> supplier, Function<? super T, ? extends Iterator<? extends V>> fn) {
		return new AbstractChannelSupplier<V>(supplier) {
			Iterator<? extends V> iterator = CollectionUtils.emptyIterator();
			boolean endOfStream;

			@Override
			protected Promise<V> doGet() {
				if (iterator.hasNext()) return Promise.of(iterator.next());
				SettablePromise<V> cb = new SettablePromise<>();
				next(cb);
				return cb;
			}

			private void next(SettablePromise<V> cb) {
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

	public static class ChannelSupplierEmpty<T> extends AbstractChannelSupplier<T> {
		@Override
		protected Promise<T> doGet() {
			return Promise.of(null);
		}
	}

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
