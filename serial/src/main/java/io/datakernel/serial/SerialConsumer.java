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
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.exception.UncheckedException;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.Recyclable.deepRecycle;
import static io.datakernel.util.Recyclable.tryRecycle;

/**
 * This interface represents consumer of data items that should be used serially (each consecutive {@link #accept(Object)}
 * operation should be called only after previous {@link #accept(Object)} operation finishes.
 * <p>
 * After consumer is closed, all subsequent calls to {@link #accept(Object)} will return stage, completed exceptionally.
 * <p>
 * If any exception is caught while consuming data items, {@link #close(Throwable)} method should
 * be called. All resources should be freed and the caught exception should be propagated to all related processes.
 * <p>
 * If {@link #accept(Object)} takes {@code null} as argument, it represents end-of-stream and means that no additional
 * data should be consumed.
 * <p>
 */

public interface SerialConsumer<T> extends Cancellable {
	Stage<Void> accept(@Nullable T value);

	default Stage<Void> accept(T item1, T item2) {
		return accept(item1)
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						tryRecycle(item2);
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
						tryRecycle(item2);
						deepRecycle(items);
						return Stage.ofException(e);
					}
				})
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						deepRecycle(items);
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
			protected Stage<Void> doAccept(T value) {
				if (value != null) {
					return thisConsumer.accept(value);
				}
				return Stage.complete();
			}
		};
	}

	static <T> SerialConsumer<T> ofException(Throwable e) {
		return new AbstractSerialConsumer<T>() {
			@Override
			protected Stage<Void> doAccept(T value) {
				tryRecycle(value);
				return Stage.ofException(e);
			}
		};
	}

	static <T> SerialConsumer<T> ofSupplier(Function<SerialSupplier<T>, MaterializedStage<Void>> supplier) {
		return ofSupplier(supplier, new SerialZeroBuffer<>());
	}

	static <T> SerialConsumer<T> ofSupplier(Function<SerialSupplier<T>, MaterializedStage<Void>> supplier, SerialQueue<T> queue) {
		MaterializedStage<Void> extraAcknowledge = supplier.apply(queue.getSupplier());
		SerialConsumer<T> result = queue.getConsumer();
		if (extraAcknowledge == Stage.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	static <T> SerialConsumer<T> ofStage(Stage<? extends SerialConsumer<T>> stage) {
		if (stage.isResult()) return stage.materialize().getResult();
		MaterializedStage<? extends SerialConsumer<T>> materializedStage = stage.materialize();
		return new AbstractSerialConsumer<T>() {
			SerialConsumer<T> consumer;
			Throwable exception;

			@Override
			protected Stage<Void> doAccept(T value) {
				if (consumer != null) return consumer.accept(value);
				return materializedStage.thenComposeEx((consumer, e) -> {
					if (e == null) {
						this.consumer = consumer;
						return consumer.accept(value);
					} else {
						tryRecycle(value);
						return Stage.ofException(e);
					}
				});
			}

			@Override
			protected void onClosed(Throwable e) {
				exception = e;
				materializedStage.whenResult(supplier -> supplier.close(e));
			}
		};
	}

	static <T> SerialConsumer<T> ofLazyProvider(Supplier<? extends SerialConsumer<T>> provider) {
		return new AbstractSerialConsumer<T>() {
			private SerialConsumer<T> consumer;

			@Override
			protected Stage<Void> doAccept(@Nullable T value) {
				if (consumer == null) consumer = provider.get();
				return consumer.accept(value);
			}

			@Override
			protected void onClosed(Throwable e) {
				if (consumer == null) consumer = provider.get();
				consumer.close(e);
			}
		};
	}


	/**
	 * Wraps {@link AsyncTcpSocket#write(ByteBuf)} operation into {@link SerialConsumer}
	 *
	 * @return {@link SerialConsumer} of  ByteBufs that will be sent to network
	 */
	static SerialConsumer<ByteBuf> ofSocket(AsyncTcpSocket socket) {
		return SerialConsumer.of(socket::write, socket)
				.withAcknowledgement(ack -> ack
						.thenCompose($ -> socket.write(null)));
	}


	default <R> R apply(SerialConsumerFunction<T, R> fn) {
		return fn.apply(this);
	}

	default SerialConsumer<T> async() {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Stage<Void> doAccept(T value) {
				return SerialConsumer.this.accept(value).async();
			}
		};
	}

	default SerialConsumer<T> withExecutor(AsyncExecutor asyncExecutor) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Stage<Void> doAccept(T value) {
				return asyncExecutor.execute(() -> SerialConsumer.this.accept(value));
			}
		};
	}

	default SerialConsumer<T> peek(Consumer<? super T> fn) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Stage<Void> doAccept(T value) {
				if (value != null) fn.accept(value);
				return SerialConsumer.this.accept(value);
			}
		};
	}

	default <V> SerialConsumer<V> transform(Function<? super V, ? extends T> fn) {
		return new AbstractSerialConsumer<V>(this) {
			@Override
			protected Stage<Void> doAccept(V value) {
				if (value != null) {
					T newValue;
					try {
						newValue = fn.apply(value);
					} catch (UncheckedException u) {
						SerialConsumer.this.close(u.getCause());
						return Stage.ofException(u.getCause());
					}
					return SerialConsumer.this.accept(newValue);
				} else {
					return SerialConsumer.this.accept(null);
				}
			}
		};
	}

	default <V> SerialConsumer<V> transformAsync(Function<? super V, ? extends Stage<T>> fn) {
		return new AbstractSerialConsumer<V>(this) {
			@Override
			protected Stage<Void> doAccept(V value) {
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
			protected Stage<Void> doAccept(T value) {
				if (value != null && predicate.test(value)) {
					return SerialConsumer.this.accept(value);
				} else {
					tryRecycle(value);
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
			protected Stage<Void> doAccept(@Nullable T value) {
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
			protected void onClosed(Throwable e) {
				acknowledgement.trySetException(e);
			}
		};
	}

	static MaterializedStage<Void> getAcknowledgement(Consumer<Function<Stage<Void>, Stage<Void>>> cb) {
		SettableStage<Void> result = new SettableStage<>();
		cb.accept(ack -> ack.whenComplete(result::set));
		return result;
	}
}
