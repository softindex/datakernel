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

package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.*;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.Recyclable.deepRecycle;

public interface SerialSupplier<T> extends Cancellable {
	Stage<T> get();

	static <T> SerialSupplier<T> ofConsumer(Consumer<SerialConsumer<T>> consumer, SerialQueue<T> queue) {
		consumer.accept(queue.getConsumer());
		return queue.getSupplier();
	}

	static <T> SerialSupplier<T> of(AsyncSupplier<T> supplier) {
		return of(supplier, null);
	}

	static <T> SerialSupplier<T> of(AsyncSupplier<T> supplier, @Nullable Cancellable cancellable) {
		return new AbstractSerialSupplier<T>(cancellable) {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return supplier.get();
			}
		};
	}

	static <T> SerialSupplier<T> of() {
		return new AbstractSerialSupplier<T>() {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return Stage.of(null);
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
			public Stage<T> get() {
				assert !isClosed();
				return Stage.ofException(e);
			}
		};
	}

	static <T> SerialSupplier<T> ofIterable(Iterable<? extends T> iterable) {
		return ofIterator(iterable.iterator());
	}

	static <T> SerialSupplier<T> ofIterator(Iterator<? extends T> iterator) {
		return new AbstractSerialSupplier<T>() {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return Stage.of(iterator.hasNext() ? iterator.next() : null);
			}

			@Override
			protected void onClosed(Throwable e) {
				deepRecycle(iterator);
			}
		};
	}

	static <T> SerialSupplier<T> ofStage(Stage<? extends SerialSupplier<T>> stage) {
		if (stage.hasResult()) return stage.getResult();
		MaterializedStage<? extends SerialSupplier<T>> materializedStage = stage.materialize();
		return new AbstractSerialSupplier<T>() {
			SerialSupplier<T> supplier;
			Throwable exception;

			@Override
			public Stage<T> get() {
				assert !isClosed();
				if (supplier != null) return supplier.get();
				return materializedStage.thenComposeEx((supplier, e) -> {
					if (e == null) {
						this.supplier = supplier;
						return supplier.get();
					} else {
						return Stage.ofException(e);
					}
				});
			}

			@Override
			protected void onClosed(Throwable e) {
				exception = e;
				materializedStage.whenResult(supplier -> supplier.closeWithError(e));
			}
		};
	}

	static <T> SerialSupplier<T> ofLazyProvider(Supplier<? extends SerialSupplier<T>> provider) {
		return new AbstractSerialSupplier<T>() {
			private SerialSupplier<T> supplier;

			@Override
			public Stage<T> get() {
				assert !isClosed();
				if (supplier == null) supplier = provider.get();
				return supplier.get();
			}

			@Override
			protected void onClosed(Throwable e) {
				if (supplier == null) supplier = provider.get();
				supplier.closeWithError(e);
			}
		};
	}

	default Stage<Void> streamTo(SerialConsumer<T> consumer) {
		return SerialSuppliers.stream(this, consumer);
	}

	default void streamTo(SerialInput<T> to) {
		to.setInput(this);
		if (to instanceof AsyncProcess) {
			getCurrentEventloop().post(((AsyncProcess) to)::start);
		}
	}

	default <A, R> Stage<R> toCollector(Collector<T, A, R> collector) {
		return SerialSuppliers.toCollector(this, collector);
	}

	default Stage<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	default <R> R apply(SerialSupplierFunction<T, R> fn) {
		return fn.apply(this);
	}

	default SerialSupplier<T> async() {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return SerialSupplier.this.get().async();
			}
		};
	}

	default SerialSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		AsyncSupplier<T> supplier = this::get;
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return asyncExecutor.execute(supplier);
			}
		};
	}

	default SerialSupplier<T> peek(Consumer<? super T> fn) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return SerialSupplier.this.get()
						.whenResult(value -> { if (value != null) fn.accept(value);});
			}
		};
	}

	default SerialSupplier<T> peekAsync(AsyncConsumer<? super T> fn) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return SerialSupplier.this.get()
						.thenCompose(item -> {
							if (item != null) {
								return fn.accept(item)
										.thenCompose($ -> Stage.of(item));
							} else {
								return Stage.of(null);
							}
						});
			}
		};
	}

	default <V> SerialSupplier<V> transform(Function<? super T, ? extends V> fn) {
		return new AbstractSerialSupplier<V>(this) {
			@Override
			public Stage<V> get() {
				assert !isClosed();
				return SerialSupplier.this.get()
						.thenApply(value -> value != null ? fn.apply(value) : null);
			}
		};
	}

	@SuppressWarnings("unchecked")
	default <V> SerialSupplier<V> transformAsync(Function<? super T, ? extends Stage<V>> fn) {
		return new AbstractSerialSupplier<V>(this) {
			@Override
			public Stage<V> get() {
				assert !isClosed();
				return SerialSupplier.this.get()
						.thenCompose(value -> value != null ?
								fn.apply(value) :
								Stage.of(null));
			}
		};
	}

	default SerialSupplier<T> filter(Predicate<? super T> predicate) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				while (true) {
					Stage<T> stage = SerialSupplier.this.get();
					if (stage.hasResult()) {
						if (predicate.test(stage.getResult())) return stage;
						continue;
					}
					return stage.thenCompose(value -> value == null || predicate.test(value) ? Stage.of(value) : get());
				}
			}
		};
	}

	default SerialSupplier<T> filterAsync(AsyncPredicate<? super T> predicate) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return SerialSupplier.this.get()
						.thenCompose(value -> value != null ?
								predicate.test(value)
										.thenCompose(testResult -> testResult ? Stage.of(value) : get()) :
								Stage.of(null));
			}
		};
	}

	default SerialSupplier<T> withEndOfStream(Function<Stage<Void>, Stage<Void>> fn) {
		SettableStage<Void> endOfStream = new SettableStage<>();
		MaterializedStage<Void> newEndOfStream = fn.apply(endOfStream).materialize();
		return new AbstractSerialSupplier<T>() {
			@SuppressWarnings("unchecked")
			@Override
			public Stage<T> get() {
				assert !isClosed();
				return SerialSupplier.this.get()
						.thenComposeEx((item, e) -> {
							if (e == null) {
								if (item != null) return Stage.of(item);
								endOfStream.trySet(null);
								return (Stage<T>) newEndOfStream;
							} else {
								endOfStream.trySetException(e);
								return (Stage<T>) newEndOfStream;
							}
						});
			}

			@Override
			protected void onClosed(Throwable e) {
				endOfStream.trySetException(e);
			}
		};
	}
}
