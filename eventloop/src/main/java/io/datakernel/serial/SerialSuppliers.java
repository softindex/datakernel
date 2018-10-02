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
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
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
			protected Stage<T> doGet() {
				T item = this.thisItem;
				this.thisItem = null;
				return Stage.of(item);
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
			protected Stage<T> doGet() {
				return current.get()
						.thenComposeEx((value, e) -> {
							if (e == null) {
								if (value != null) {
									return Stage.of(value);
								} else {
									if (iterator.hasNext()) {
										current = iterator.next();
										return get();
									} else {
										return Stage.of(null);
									}
								}
							} else {
								while (iterator.hasNext()) {
									iterator.next().closeWithError(e);
								}
								return Stage.ofException(e);
							}
						});
			}

			@Override
			protected void onClosed(Throwable e) {
				current.closeWithError(e);
				while (iterator.hasNext()) {
					iterator.next().closeWithError(e);
				}
			}
		};
	}

	protected static <T, A, R> Stage<R> toCollector(SerialSupplier<T> supplier, Collector<T, A, R> collector) {
		SettableStage<R> cb = new SettableStage<>();
		toCollectorImpl(supplier, collector.supplier().get(), collector.accumulator(), collector.finisher(), cb);
		return cb;
	}

	private static <T, A, R> void toCollectorImpl(SerialSupplier<T> supplier,
			A accumulatedValue, BiConsumer<A, T> accumulator, Function<A, R> finisher,
			SettableStage<R> result) {
		Stage<T> stage;
		while (true) {
			stage = supplier.get();
			if (!stage.hasResult()) break;
			T item = stage.getResult();
			if (item != null) {
				accumulator.accept(accumulatedValue, item);
				continue;
			}
			break;
		}
		stage.whenComplete((value, e) -> {
			if (e == null) {
				if (value != null) {
					accumulator.accept(accumulatedValue, value);
					toCollectorImpl(supplier, accumulatedValue, accumulator, finisher, result);
				} else {
					result.set(finisher.apply(accumulatedValue));
				}
			} else {
				deepRecycle(finisher.apply(accumulatedValue));
				result.setException(e);
			}
		});
	}

	public static <T> MaterializedStage<Void> stream(SerialSupplier<T> supplier, SerialConsumer<T> consumer) {
		SettableStage<Void> cb = new SettableStage<>();
		streamImpl(supplier, consumer, cb);
		return cb;
	}

	private static <T> void streamImpl(SerialSupplier<T> supplier, SerialConsumer<T> consumer, SettableStage<Void> result) {
		Stage<T> supplierStage;
		while (true) {
			supplierStage = supplier.get();
			if (!supplierStage.hasResult()) break;
			T item = supplierStage.getResult();
			if (item == null) break;
			Stage<Void> consumerStage = consumer.accept(item);
			if (consumerStage.isResult()) continue;
			consumerStage.whenComplete(($, e) -> {
				if (e == null) {
					streamImpl(supplier, consumer, result);
				} else {
					supplier.closeWithError(e);
					result.trySetException(e);
				}
			});
			return;
		}
		supplierStage
				.whenComplete((item, e1) -> {
					if (e1 == null) {
						consumer.accept(item)
								.whenComplete(($, e2) -> {
									if (e2 == null) {
										if (item != null) {
											streamImpl(supplier, consumer, result);
										} else {
											result.trySet(null);
										}
									} else {
										supplier.closeWithError(e2);
										result.trySetException(e2);
									}
								});
					} else {
						consumer.closeWithError(e1);
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
					private SettableStage<T> pending;

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
											SettableStage<T> pending = this.pending;
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
										closeWithError(e);
									}
								});
					}

					@SuppressWarnings("unchecked")
					@Override
					protected Stage<T> doGet() {
						assert pending == null;
						if (!deque.isEmpty() || endOfStream) {
							T result = deque.poll();
							tryPrefetch();
							return Stage.of(result);
						}
						SettableStage<T> pending = new SettableStage<>();
						this.pending = pending;
						tryPrefetch();
						return pending;
					}

					@Override
					protected void onClosed(Throwable e) {
						deepRecycle(deque);
						actual.closeWithError(e);
						if (pending != null) {
							pending.trySetException(e);
							pending = null;
						}
					}
				};
	}

	@SuppressWarnings("unchecked")
	public static <T> SerialSupplier<T> prefetch(SerialSupplier<? extends T> actual) {
		return new AbstractSerialSupplier<T>() {
			private T prefetched;
			private boolean endOfStream;
			private boolean prefetching;
			@Nullable
			private SettableStage<T> pending;

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
									SettableStage<T> pending = this.pending;
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
								closeWithError(e);
							}
						});
			}

			@SuppressWarnings("unchecked")
			@Override
			protected Stage<T> doGet() {
				assert pending == null;
				if (prefetched != null || endOfStream) {
					T result = this.prefetched;
					this.prefetched = null;
					tryPrefetch();
					return Stage.of(result);
				}
				SettableStage<T> pending = new SettableStage<>();
				this.pending = pending;
				tryPrefetch();
				return pending;
			}

			@Override
			protected void onClosed(Throwable e) {
				tryRecycle(prefetched);
				prefetched = null;
				actual.closeWithError(e);
				if (pending != null) {
					pending.setException(e);
					pending = null;
				}
			}
		};
	}

}
