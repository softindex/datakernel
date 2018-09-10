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

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.Recyclable.deepRecycle;

public interface SerialConsumer<T> extends Cancellable {
	Stage<Void> accept(@Nullable T value);

	default Stage<Void> accept(T item1, T item2) {
		return accept(item1)
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						deepRecycle(item2);
						return Stage.ofException(e);
					}
				});
	}

	@SuppressWarnings("unchecked")
	default Stage<Void> accept(T item1, T item2, T... items) {
		return accept(item1)
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item1);
					} else {
						deepRecycle(item1);
						for (T item : items) {
							deepRecycle(item);
						}
						return Stage.ofException(e);
					}
				})
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						for (T item : items) {
							deepRecycle(item);
						}
						return Stage.ofException(e);
					}
				})
				.thenCompose($ -> acceptAll(asIterator(items)));
	}

	default Stage<Void> acceptAll(Iterator<? extends T> it) {
		return SerialConsumers.acceptAll(this, it);
	}

	default Stage<Void> acceptAll(Iterable<T> iterable) {
		return acceptAll(iterable.iterator());
	}

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer) {
		return of(consumer, e -> {});
	}

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer, Cancellable cancellable) {
		return new AbstractSerialConsumer<T>(cancellable) {
			final AsyncConsumer<T> thisConsumer = consumer;

			@Override
			public Stage<Void> accept(T value) {
				if (value != null) {
					return thisConsumer.accept(value);
				}
				return Stage.complete();
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
				deepRecycle(value);
				return Stage.ofException(e);
			}
		};
	}

	static <T> SerialConsumer<T> ofSupplier(Consumer<SerialSupplier<T>> supplierAcceptor, SerialQueue<T> queue) {
		supplierAcceptor.accept(queue.getSupplier());
		return queue.getConsumer();
	}

	static <T> SerialConsumer<T> ofStage(Stage<? extends SerialConsumer<T>> stage) {
		if (stage.hasResult()) return stage.getResult();
		MaterializedStage<? extends SerialConsumer<T>> materializedStage = stage.materialize();
		return new AbstractSerialConsumer<T>() {
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
						deepRecycle(value);
						return Stage.ofException(e);
					}
				});
			}

			@Override
			protected void onClose(Throwable e) {
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
				if (value != null) fn.accept(value);
				return SerialConsumer.this.accept(value);
			}
		};
	}

	default SerialConsumer<T> peekAsync(AsyncConsumer<? super T> fn) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return value != null ?
						Stages.all(SerialConsumer.this.accept(value), fn.accept(value)) :
						SerialConsumer.this.accept(null);
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
				if (value != null) {
					return predicate.test(value)
							.thenCompose(test -> test ?
									SerialConsumer.this.accept(value) :
									Stage.complete());
				} else {
					return Stage.complete();
				}
			}
		};
	}

	default SerialConsumer<T> withAcknowledgement(Function<Stage<Void>, Stage<Void>> fn) {
		SettableStage<Void> acknowledgement = new SettableStage<>();
		MaterializedStage<Void> newAcknowledgement = fn.apply(acknowledgement).materialize();
		return new AbstractSerialConsumer<T>() {
			@Override
			public Stage<Void> accept(@Nullable T value) {
				if (value != null) {
					return SerialConsumer.this.accept(value)
							.thenComposeEx(($, e) -> {
								if (e == null) {
									return Stage.complete();
								}
								acknowledgement.trySetException(e);
								return newAcknowledgement;
							});
				} else {
					SerialConsumer.this.accept(null).whenComplete(acknowledgement::trySet);
					return newAcknowledgement;
				}
			}

			@Override
			protected void onClose(Throwable e) {
				acknowledgement.trySetException(e);
			}
		};
	}
}
