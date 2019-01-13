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

package io.datakernel.csp;

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.dsl.ChannelConsumerTransformer;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.exception.UncheckedException;
import org.jetbrains.annotations.Nullable;

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
 * After consumer is closed, all subsequent calls to {@link #accept(Object)} will return promise, completed exceptionally.
 * <p>
 * If any exception is caught while consuming data items, {@link #close(Throwable)} method should
 * be called. All resources should be freed and the caught exception should be propagated to all related processes.
 * <p>
 * If {@link #accept(Object)} takes {@code null} as argument, it represents end-of-stream and means that no additional
 * data should be consumed.
 * <p>
 */

public interface ChannelConsumer<T> extends Cancellable {
	Promise<Void> accept(@Nullable T value);

	default Promise<Void> accept(@Nullable T item1, @Nullable T item2) {
		return accept(item1)
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						tryRecycle(item2);
						return Promise.ofException(e);
					}
				});
	}

	@SuppressWarnings("unchecked")
	default Promise<Void> accept(T item1, T item2, T... items) {
		return accept(item1)
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item1);
					} else {
						tryRecycle(item2);
						deepRecycle(items);
						return Promise.ofException(e);
					}
				})
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						deepRecycle(items);
						return Promise.ofException(e);
					}
				})
				.thenCompose($ -> acceptAll(asIterator(items)));
	}

	default Promise<Void> acceptAll(Iterator<? extends T> it) {
		return ChannelConsumers.acceptAll(this, it);
	}

	default Promise<Void> acceptAll(Iterable<T> iterable) {
		return acceptAll(iterable.iterator());
	}

	static <T> ChannelConsumer<T> ofConsumer(Consumer<T> consumer) {
		return of(AsyncConsumer.of(consumer));
	}

	static <T> ChannelConsumer<T> of(AsyncConsumer<T> consumer) {
		return of(consumer, e -> {});
	}

	static <T> ChannelConsumer<T> of(AsyncConsumer<T> consumer, Cancellable cancellable) {
		return new AbstractChannelConsumer<T>(cancellable) {
			final AsyncConsumer<T> thisConsumer = consumer;

			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) {
					return thisConsumer.accept(value);
				}
				return Promise.complete();
			}
		};
	}

	static <T> ChannelConsumer<T> ofException(Throwable e) {
		return new AbstractChannelConsumer<T>() {
			@Override
			protected Promise<Void> doAccept(T value) {
				tryRecycle(value);
				return Promise.ofException(e);
			}
		};
	}

	static <T> ChannelConsumer<T> ofSupplier(Function<ChannelSupplier<T>, MaterializedPromise<Void>> supplier) {
		return ofSupplier(supplier, new ChannelZeroBuffer<>());
	}

	static <T> ChannelConsumer<T> ofSupplier(Function<ChannelSupplier<T>, MaterializedPromise<Void>> supplier, ChannelQueue<T> queue) {
		MaterializedPromise<Void> extraAcknowledge = supplier.apply(queue.getSupplier());
		ChannelConsumer<T> result = queue.getConsumer();
		if (extraAcknowledge == Promise.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	static <T> ChannelConsumer<T> ofPromise(Promise<? extends ChannelConsumer<T>> promise) {
		if (promise.isResult()) return promise.materialize().getResult();
		MaterializedPromise<? extends ChannelConsumer<T>> materializedPromise = promise.materialize();
		return new AbstractChannelConsumer<T>() {
			ChannelConsumer<T> consumer;
			Throwable exception;

			@Override
			protected Promise<Void> doAccept(T value) {
				if (consumer != null) return consumer.accept(value);
				return materializedPromise.thenComposeEx((consumer, e) -> {
					if (e == null) {
						this.consumer = consumer;
						return consumer.accept(value);
					} else {
						tryRecycle(value);
						return Promise.ofException(e);
					}
				});
			}

			@Override
			protected void onClosed(Throwable e) {
				exception = e;
				materializedPromise.whenResult(supplier -> supplier.close(e));
			}
		};
	}

	static <T> ChannelConsumer<T> ofLazyProvider(Supplier<? extends ChannelConsumer<T>> provider) {
		return new AbstractChannelConsumer<T>() {
			private ChannelConsumer<T> consumer;

			@Override
			protected Promise<Void> doAccept(@Nullable T value) {
				if (consumer == null) consumer = provider.get();
				return consumer.accept(value);
			}

			@Override
			protected void onClosed(Throwable e) {
				if (consumer != null) {
					consumer.close(e);
				}
			}
		};
	}

	/**
	 * Wraps {@link AsyncTcpSocket#write(ByteBuf)} operation into {@link ChannelConsumer}
	 *
	 * @return {@link ChannelConsumer} of  ByteBufs that will be sent to network
	 */
	static ChannelConsumer<ByteBuf> ofSocket(AsyncTcpSocket socket) {
		return ChannelConsumer.of(socket::write, socket)
				.withAcknowledgement(ack -> ack
						.thenCompose($ -> socket.write(null)));
	}


	default <R> R transformWith(ChannelConsumerTransformer<T, R> fn) {
		return fn.transform(this);
	}

	default ChannelConsumer<T> async() {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return ChannelConsumer.this.accept(value).async();
			}
		};
	}

	default ChannelConsumer<T> withExecutor(AsyncExecutor asyncExecutor) {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return asyncExecutor.execute(() -> ChannelConsumer.this.accept(value));
			}
		};
	}

	default ChannelConsumer<T> peek(Consumer<? super T> fn) {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) fn.accept(value);
				return ChannelConsumer.this.accept(value);
			}
		};
	}

	default <V> ChannelConsumer<V> map(Function<? super V, ? extends T> fn) {
		return new AbstractChannelConsumer<V>(this) {
			@Override
			protected Promise<Void> doAccept(V value) {
				if (value != null) {
					T newValue;
					try {
						newValue = fn.apply(value);
					} catch (UncheckedException u) {
						ChannelConsumer.this.close(u.getCause());
						return Promise.ofException(u.getCause());
					}
					return ChannelConsumer.this.accept(newValue);
				} else {
					return ChannelConsumer.this.accept(null);
				}
			}
		};
	}

	default <V> ChannelConsumer<V> mapAsync(Function<? super V, ? extends Promise<T>> fn) {
		return new AbstractChannelConsumer<V>(this) {
			@Override
			protected Promise<Void> doAccept(V value) {
				return value != null ?
						fn.apply(value)
								.thenCompose(ChannelConsumer.this::accept) :
						ChannelConsumer.this.accept(null);
			}
		};
	}

	default ChannelConsumer<T> filter(Predicate<? super T> predicate) {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null && predicate.test(value)) {
					return ChannelConsumer.this.accept(value);
				} else {
					tryRecycle(value);
					return Promise.complete();
				}
			}
		};
	}

	default ChannelConsumer<T> withAcknowledgement(Function<Promise<Void>, Promise<Void>> fn) {
		SettablePromise<Void> acknowledgement = new SettablePromise<>();
		MaterializedPromise<Void> newAcknowledgement = fn.apply(acknowledgement).materialize();
		return new AbstractChannelConsumer<T>() {
			@Override
			protected Promise<Void> doAccept(@Nullable T value) {
				if (value != null) {
					return ChannelConsumer.this.accept(value)
							.thenComposeEx(($, e) -> {
								if (e == null) {
									return Promise.complete();
								}
								acknowledgement.trySetException(e);
								return newAcknowledgement;
							});
				} else {
					ChannelConsumer.this.accept(null).whenComplete(acknowledgement::trySet);
					return newAcknowledgement;
				}
			}

			@Override
			protected void onClosed(Throwable e) {
				acknowledgement.trySetException(e);
			}
		};
	}

	static MaterializedPromise<Void> getAcknowledgement(Consumer<Function<Promise<Void>, Promise<Void>>> cb) {
		SettablePromise<Void> result = new SettablePromise<>();
		cb.accept(ack -> ack.whenComplete(result::set));
		return result;
	}
}
