package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.util.CollectionUtils;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

public final class SerialSuppliers {
	private SerialSuppliers() {}

	public static <T> SerialSupplier<T> of(T item) {
		return new AbstractSerialSupplier<T>() {
			@Nullable
			T thisItem = item;

			@Override
			public Stage<T> get() {
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
			public Stage<T> get() {
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
		return Stage.ofCallback(cb -> toCollectorImpl(supplier,
				collector.supplier().get(), collector.accumulator(), collector.finisher(), cb));
	}

	private static <T, A, R> void toCollectorImpl(SerialSupplier<T> supplier,
			A accumulatedValue, BiConsumer<A, T> accumulator, Function<A, R> finisher,
			SettableStage<R> result) {
		supplier.get(value -> accumulator.accept(accumulatedValue, value))
				.whenComplete((value, e) -> {
					if (e == null) {
						if (value != null) {
							accumulator.accept(accumulatedValue, value);
							toCollectorImpl(supplier, accumulatedValue, accumulator, finisher, result);
						} else {
							result.set(finisher.apply(accumulatedValue));
						}
					} else {
						result.setException(e);
					}
				});
	}

	public static <T> Stage<Void> stream(SerialSupplier<T> supplier, SerialConsumer<T> consumer) {
		return Stage.ofCallback(cb -> streamImpl(supplier, consumer, cb));
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

}
