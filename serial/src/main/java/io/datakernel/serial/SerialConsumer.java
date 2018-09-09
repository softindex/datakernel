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
import io.datakernel.util.Recyclable;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface SerialConsumer<T> extends Cancellable {
	Stage<Void> accept(@Nullable T value);

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer) {
		return of(consumer, e -> {});
	}

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer, Cancellable cancellable) {
		SettableStage<Void> endOfStream = new SettableStage<>();
		return new SerialConsumer<T>() {
			final AsyncConsumer<T> thisConsumer = consumer;

			@Override
			public Stage<Void> accept(T value) {
				if (value != null) {
					return thisConsumer.accept(value);
				}
				return Stage.complete();
			}

			@Override
			public void closeWithError(Throwable e) {
				cancellable.closeWithError(e);
			}
		};
	}

	static <T> SerialConsumer<T> recycle() {
		return of(AsyncConsumer.of(Recyclable::deepRecycle));
	}

	static <T> SerialConsumer<T> ofException(Throwable e) {
		return new AbstractSerialConsumer<T>() {
			@Override
			public Stage<Void> accept(T value) {
				Recyclable.deepRecycle(value);
				return Stage.ofException(e);
			}
		};
	}

	static <T> SerialConsumer<T> ofSupplier(Consumer<SerialSupplier<T>> supplierAcceptor, SerialQueue<T> queue) {
		supplierAcceptor.accept(queue.getSupplier());
		return queue.getConsumer();
	}

	static <T> SerialConsumer<T> ofStage(Stage<? extends SerialConsumer<T>> stage) {
		MaterializedStage<? extends SerialConsumer<T>> materializedStage = stage.materialize();
		return new SerialConsumer<T>() {
			SerialConsumer<T> consumer;
			Throwable exception;

			@Override
			public Stage<Void> accept(T value) {
				if (consumer != null) return consumer.accept(value);
				return materializedStage.thenComposeEx((consumer, e) -> {
					if (e == null) {
						this.consumer = consumer;
						return consumer.accept(value);
					} else {
						Recyclable.deepRecycle(value);
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

	default <R> SerialConsumer<R> apply(SerialConsumerModifier<T, R> modifier) {
		return apply((Function<SerialConsumer<T>, SerialConsumer<R>>) modifier::apply);
	}

	default <R> R apply(Function<SerialConsumer<T>, R> fn) {
		return fn.apply(this);
	}

	default SerialConsumer<T> async() {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return SerialConsumer.this.accept(value).async();
			}
		};
	}

	default SerialConsumer<T> withExecutor(AsyncExecutor asyncExecutor) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return asyncExecutor.execute(() -> SerialConsumer.this.accept(value));
			}
		};
	}

	default SerialConsumer<T> peek(Consumer<? super T> fn) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				fn.accept(value);
				return SerialConsumer.this.accept(value);
			}
		};
	}

	default SerialConsumer<T> peekAsync(AsyncConsumer<? super T> fn) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return fn.accept(value)
						.whenException(this::closeWithError)
						.thenCompose($ -> SerialConsumer.this.accept(value));
			}
		};
	}

	default <V> SerialConsumer<V> transform(Function<? super V, ? extends T> fn) {
		return new AbstractSerialConsumer<V>(this) {
			@Override
			public Stage<Void> accept(V value) {
				return SerialConsumer.this.accept(value != null ? fn.apply(value) : null);
			}
		};
	}

	default <V> SerialConsumer<V> transformAsync(Function<? super V, ? extends Stage<T>> fn) {
		return new AbstractSerialConsumer<V>(this) {
			@Override
			public Stage<Void> accept(V value) {
				return value != null ?
						fn.apply(value)
								.thenCompose(SerialConsumer.this::accept) :
						SerialConsumer.this.accept(null);
			}
		};
	}

	default SerialConsumer<T> filter(Predicate<? super T> predicate) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				if (value != null && predicate.test(value)) {
					return SerialConsumer.this.accept(value);
				} else {
					return Stage.complete();
				}
			}
		};
	}

	default SerialConsumer<T> filterAsync(AsyncPredicate<? super T> predicate) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				if (value == null) {
					return Stage.complete();
				}
				return predicate.test(value)
						.thenCompose(test -> test ?
								SerialConsumer.this.accept(value) :
								Stage.complete());
			}
		};
	}

	default SerialConsumer<T> whenException(Consumer<Throwable> action) {
		return new AbstractSerialConsumer<T>(this) {
			boolean done = false;

			private void fire(Throwable e) {
				if (!done) {
					done = true;
					action.accept(e);
				}
			}

			@Override
			public Stage<Void> accept(T value) {
				return SerialConsumer.this.accept(value)
						.whenComplete(($, e) -> {
							if (e != null) {
								fire(e);
							}
						});
			}

			@Override
			public void closeWithError(Throwable e) {
				super.closeWithError(e);
				fire(e);
			}
		};
	}

	default SerialConsumer<T> withAcknowledgement(Function<Stage<Void>, Stage<Void>> fn) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(@Nullable T value) {
				if (value == null) {
					return fn.apply(SerialConsumer.this.accept(null));
				}
				return SerialConsumer.this.accept(value)
						.thenComposeEx(($, e) -> e != null ?
								fn.apply(Stage.ofException(e)) :
								Stage.complete());
			}
		};
	}

	//** whenComplete lambda has dummy void argument for API compatibility **//
	default SerialConsumer<T> whenComplete(BiConsumer<Void, Throwable> action) {
		return new AbstractSerialConsumer<T>(this) {
			boolean done = false;

			private void fire(Throwable e) {
				if (!done) {
					done = true;
					action.accept(null, e);
				}
			}

			@Override
			public Stage<Void> accept(T value) {
				return SerialConsumer.this.accept(value).whenComplete(($, e) -> fire(e));
			}

			@Override
			public void closeWithError(Throwable e) {
				super.closeWithError(e);
				fire(e);
			}
		};
	}
}
