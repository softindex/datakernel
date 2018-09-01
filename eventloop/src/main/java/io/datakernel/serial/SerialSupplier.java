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

import io.datakernel.async.*;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.Recyclable.deepRecycle;

public interface SerialSupplier<T> extends Cancellable {
	Stage<T> get();

	default Stage<T> get(Consumer<T> preprocessor) {
		Stage<T> stage;
		while (true) {
			stage = this.get();
			if (!stage.hasResult()) break;
			T item = stage.getResult();
			if (item != null) {
				preprocessor.accept(item);
				continue;
			}
			break;
		}
		return stage;
	}

	default SerialSupplier<T> with(UnaryOperator<SerialSupplier<T>> modifier) {
		return modifier.apply(this);
	}

	default <X> X streamTo(Function<SerialSupplier<T>, X> fn) {
		SerialZeroBuffer<T> buffer = new SerialZeroBuffer<>();
		streamTo(buffer.getConsumer());
		return fn.apply(buffer.getSupplier());
	}

	static <T> SerialSupplier<T> of(AsyncSupplier<T> supplier) {
		return of(supplier, null);
	}

	static <T> SerialSupplier<T> of(AsyncSupplier<T> supplier, Cancellable cancellable) {
		return new AbstractSerialSupplier<T>(cancellable) {
			@Override
			public Stage<T> get() {
				return supplier.get();
			}
		};
	}

	static <T> SerialSupplier<T> of() {
		return new AbstractSerialSupplier<T>() {
			@Override
			public Stage<T> get() {
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
				return Stage.ofException(e);
			}
		};
	}

	static <T> SerialSupplier<T> ofIterable(Iterable<? extends T> iterable) {
		return ofIterator(iterable.iterator());
	}

	static <T> SerialSupplier<T> ofIterator(Iterator<? extends T> iterator) {
		return new SerialSupplier<T>() {
			@Override
			public Stage<T> get() {
				return Stage.of(iterator.hasNext() ? iterator.next() : null);
			}

			@Override
			public void closeWithError(Throwable e) {
				deepRecycle(iterator);
			}
		};
	}

	static <T> SerialSupplier<T> ofStage(Stage<? extends SerialSupplier<T>> stage) {
		MaterializedStage<? extends SerialSupplier<T>> materializedStage = stage.materialize();
		return new SerialSupplier<T>() {
			SerialSupplier<T> supplier;
			Throwable exception;

			@Override
			public Stage<T> get() {
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
			public void closeWithError(Throwable e) {
				exception = e;
				materializedStage.whenResult(supplier -> supplier.closeWithError(e));
			}
		};
	}

	default SerialSupplier<T> async() {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				return SerialSupplier.this.get().async();
			}
		};
	}

	default SerialSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		AsyncSupplier<T> supplier = this::get;
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				return asyncExecutor.execute(supplier);
			}
		};
	}

	default <V> SerialSupplier<V> transform(Function<? super T, ? extends V> fn) {
		return new AbstractSerialSupplier<V>(this) {
			@Override
			public Stage<V> get() {
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
				return SerialSupplier.this.get()
						.thenCompose(value -> value == null || predicate.test(value) ? Stage.of(value) : get());
			}
		};
	}

	default SerialSupplier<T> filterAsync(AsyncPredicate<? super T> predicate) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				return SerialSupplier.this.get()
						.thenCompose(value -> value != null ?
								predicate.test(value)
										.thenCompose(test -> test ? Stage.of(value) : get()) :
								Stage.of(null));
			}
		};
	}

	default SerialSupplier<T> whenEndOfStream(Runnable action) {
		return new AbstractSerialSupplier<T>(this) {
			boolean done;

			@Override
			public Stage<T> get() {
				return SerialSupplier.this.get().whenComplete((value, e) -> {
					if (e == null && value == null && !done) {
						done = true;
						action.run();
					}
				});
			}
		};
	}

	default SerialSupplier<T> whenException(Consumer<Throwable> action) {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				return SerialSupplier.this.get().whenException(action);
			}
		};
	}

	default Stage<Void> streamTo(SerialConsumer<T> consumer) {
		return SerialSuppliers.stream(this, consumer);
	}

	default <A, R> Stage<R> toCollector(Collector<T, A, R> collector) {
		return SerialSuppliers.toCollector(this, collector);
	}

	default Stage<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

}
