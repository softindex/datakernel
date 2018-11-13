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
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.exception.UncheckedException;
import io.datakernel.util.CollectionUtils;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.datakernel.util.Recyclable.deepRecycle;
import static io.datakernel.util.Recyclable.tryRecycle;

public final class SerialSuppliers {
	private SerialSuppliers() {}

	public static <T> SerialSupplier<T> of(T item) {
		return new AbstractSerialSupplier<T>() {
			@Nullable
			T thisItem = item;

			@Override
			protected Promise<T> doGet() {
				T item = thisItem;
				thisItem = null;
				return Promise.of(item);
			}
		};
	}

	public static <T> SerialSupplier<T> concat(SerialSupplier<? extends T> supplier1, SerialSupplier<? extends T> supplier2) {
		return concat(CollectionUtils.asIterator(supplier1, supplier2));
	}

	@SafeVarargs
	public static <T> SerialSupplier<T> concat(SerialSupplier<? extends T>... suppliers) {
		return concat(CollectionUtils.asIterator(suppliers));
	}

	public static <T> SerialSupplier<T> concat(Iterator<? extends SerialSupplier<? extends T>> iterator) {
		return new AbstractSerialSupplier<T>() {
			SerialSupplier<? extends T> current = SerialSupplier.of();

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
			protected void onClosed(Throwable e) {
				current.close(e);
				while (iterator.hasNext()) {
					iterator.next().close(e);
				}
			}
		};
	}

	protected static <T, A, R> Promise<R> toCollector(SerialSupplier<T> supplier, Collector<T, A, R> collector) {
		SettablePromise<R> cb = new SettablePromise<>();
		toCollectorImpl(supplier, collector.supplier().get(), collector.accumulator(), collector.finisher(), cb);
		return cb;
	}

	private static <T, A, R> void toCollectorImpl(SerialSupplier<T> supplier,
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

	public static <T> MaterializedPromise<Void> streamTo(SerialSupplier<T> supplier, SerialConsumer<T> consumer) {
		SettablePromise<Void> cb = new SettablePromise<>();
		streamToImpl(supplier, consumer, cb);
		return cb;
	}

	private static <T> void streamToImpl(SerialSupplier<T> supplier, SerialConsumer<T> consumer, SettablePromise<Void> result) {
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
	public static <T> SerialSupplier<T> prefetch(int count, SerialSupplier<? extends T> actual) {
		return count == 0 ?
				(SerialSupplier<T>) actual :
				new AbstractSerialSupplier<T>() {
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
					protected void onClosed(Throwable e) {
						deepRecycle(deque);
						actual.close(e);
						if (pending != null) {
							pending.trySetException(e);
							pending = null;
						}
					}
				};
	}

	public static <T> SerialSupplier<T> prefetch(SerialSupplier<? extends T> actual) {
		return new AbstractSerialSupplier<T>() {
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
			protected void onClosed(Throwable e) {
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

	public static <T, V> SerialSupplier<V> remap(SerialSupplier<T> supplier, Function<? super T, ? extends Iterator<? extends V>> fn) {
		return new AbstractSerialSupplier<V>(supplier) {
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

}
