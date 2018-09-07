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

import java.util.function.*;

public interface SerialConsumer<T> extends Cancellable {
	Stage<Void> accept(@Nullable T value);

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer) {
		return of(consumer, endOfStream -> Stage.complete());
	}

	static <T> SerialConsumer<T> idle() {
		return of($ -> Stage.complete(), $ -> Stage.complete());
	}

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer,
			Function<Stage<Void>, ? extends Stage<Void>> endOfStreamHandler) {
		SettableStage<Void> endOfStream = new SettableStage<>();
		Stage<Void> endOfStreamAck = endOfStreamHandler.apply(endOfStream);
		return new SerialConsumer<T>() {
			final AsyncConsumer<T> thisConsumer = consumer;

			@Override
			public Stage<Void> accept(T value) {
				if (value != null) {
					return thisConsumer.accept(value);
				}
				endOfStream.trySet(null);
				return endOfStreamAck;
			}

			@Override
			public void closeWithError(Throwable e) {
				endOfStream.trySetException(e);
			}
		};
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
		return apply((Function<SerialConsumer<T>, SerialConsumer<R>>) modifier::applyTo);
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

	default <V> SerialConsumer<V> transform(Function<? super V, ? extends T> fn) {
		return new AbstractSerialConsumer<V>(this) {
			@Override
			public Stage<Void> accept(V value) {
				return SerialConsumer.this.accept(value != null ? fn.apply(value) : null);
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

	default SerialConsumer<T> peekAsync(AsyncConsumer<? super T> fn) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return fn.accept(value)
						.thenCompose($ -> SerialConsumer.this.accept(value));
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

	default SerialConsumer<T> whenEndOfStream(Runnable action) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				Stage<Void> result = SerialConsumer.this.accept(value);
				if (value == null) action.run();
				return result;
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

	//** Methods below accept lambdas with dummy void argument for API compatibility **//

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

	default SerialConsumer<T> thenCompose(Function<Void, Stage<Void>> action) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				Stage<Void> result = SerialConsumer.this.accept(value);
				return value != null ? result : result.thenCompose(action);
			}
		};
	}

	default SerialConsumer<T> thenComposeEx(BiFunction<Void, Throwable, Stage<Void>> action) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return SerialConsumer.this.accept(value)
						.thenComposeEx(value != null ?
								($, e) -> e != null ?
										action.apply(null, e) :
										Stage.complete() :
								action);
			}

			@Override
			public void closeWithError(Throwable e) {
				Stage<Void> res = action.apply(null, e);
				super.closeWithError(res.hasException() ? res.getException() : e); // ugh
			}
		};
	}

}
