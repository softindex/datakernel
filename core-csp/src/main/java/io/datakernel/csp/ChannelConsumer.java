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

import io.datakernel.async.function.AsyncConsumer;
import io.datakernel.async.process.AsyncExecutor;
import io.datakernel.async.process.Cancellable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.csp.dsl.ChannelConsumerTransformer;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.common.Recyclable.deepRecycle;
import static io.datakernel.common.Recyclable.tryRecycle;
import static io.datakernel.common.collection.CollectionUtils.asIterator;

/**
 * This interface represents consumer of data items that should be used serially
 * (each consecutive {@link #accept(Object)} operation should be called only after
 * previous {@link #accept(Object)} operation finishes.
 * <p>
 * After consumer is closed, all subsequent calls to {@link #accept(Object)} will
 * return a completed exceptionally promise.
 * <p>
 * If any exception is caught while consuming data items, {@link #close(Throwable)}
 * method should be called. All resources should be freed and the caught exception
 * should be propagated to all related processes.
 * <p>
 * If {@link #accept(Object)} takes {@code null} as argument, it represents end-of-stream
 * and means that no additional data should be consumed.
 */

public interface ChannelConsumer<T> extends Cancellable {
	/**
	 * Consumes a provided value and returns a
	 * {@link Promise} as a marker of success.
	 */
	@NotNull
	Promise<Void> accept(@Nullable T value);

	/**
	 * Accepts two items and returns a {@code Promise} as a
	 * marker of completion. If the first item was accepted
	 * with an exception, second item will be recycled and
	 * a Promise of exception will be returned.
	 */
	@NotNull
	default Promise<Void> accept(@Nullable T item1, @Nullable T item2) {
		return accept(item1)
				.thenEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						tryRecycle(item2);
						return Promise.ofException(e);
					}
				});
	}

	/**
	 * Accepts provided items and returns {@code Promise} as a
	 * marker of completion. If one of the items was accepted
	 * with an error, subsequent items will be recycled and a
	 * {@code Promise} of exception will be returned.
	 */
	@NotNull
	@SuppressWarnings("unchecked")
	default Promise<Void> accept(T item1, T item2, T... items) {
		return accept(item1)
				.thenEx(($, e) -> {
					if (e == null) {
						return accept(item1);
					} else {
						tryRecycle(item2);
						deepRecycle(items);
						return Promise.ofException(e);
					}
				})
				.thenEx(($, e) -> {
					if (e == null) {
						return accept(item2);
					} else {
						deepRecycle(items);
						return Promise.ofException(e);
					}
				})
				.then($ -> acceptAll(asIterator(items)));
	}

	/**
	 * @see ChannelConsumers#acceptAll(ChannelConsumer, Iterator)
	 */
	@NotNull
	default Promise<Void> acceptAll(@NotNull Iterator<? extends T> it) {
		return ChannelConsumers.acceptAll(this, it);
	}

	/**
	 * @see #acceptAll(Iterator)
	 */
	default Promise<Void> acceptAll(@NotNull Iterable<T> iterable) {
		return acceptAll(iterable.iterator());
	}

	/**
	 * Wraps {@link AsyncConsumer} in {@code ChannelConsumer}.
	 *
	 * @see ChannelConsumer#of(AsyncConsumer, Cancellable)
	 */
	static <T> ChannelConsumer<T> of(@NotNull AsyncConsumer<T> consumer) {
		return of(consumer, e -> {});
	}

	/**
	 * Wraps {@link AsyncConsumer} in {@code ChannelConsumer}.
	 *
	 * @param consumer    AsyncConsumer to be wrapped
	 * @param cancellable a Cancellable, which will be set to the returned ChannelConsumer
	 * @param <T>         type of data to be consumed
	 * @return AbstractChannelConsumer which wraps AsyncConsumer
	 */
	static <T> ChannelConsumer<T> of(@NotNull AsyncConsumer<T> consumer, @Nullable Cancellable cancellable) {
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

	/**
	 * Wraps Java's {@link Consumer} in {@code ChannelConsumer}.
	 */
	static <T> ChannelConsumer<T> ofConsumer(@NotNull Consumer<T> consumer) {
		return of(AsyncConsumer.of(consumer));
	}

	/**
	 * Creates a consumer which always returns Promise
	 * of exception when accepts values.
	 *
	 * @param e   an exception which is wrapped in returned
	 *            Promise when {@code accept()} is called
	 * @param <T> type of data to be consumed
	 * @return an AbstractChannelConsumer which always
	 * returns Promise of exception when accepts values
	 */
	static <T> ChannelConsumer<T> ofException(Throwable e) {
		return new AbstractChannelConsumer<T>() {
			@Override
			protected Promise<Void> doAccept(T value) {
				tryRecycle(value);
				return Promise.ofException(e);
			}
		};
	}

	/**
	 * @see #ofSupplier(Function, ChannelQueue)
	 */
	static <T> ChannelConsumer<T> ofSupplier(Function<ChannelSupplier<T>, Promise<Void>> supplier) {
		return ofSupplier(supplier, new ChannelZeroBuffer<>());
	}

	static <T> ChannelConsumer<T> ofSupplier(Function<ChannelSupplier<T>, Promise<Void>> supplier, ChannelQueue<T> queue) {
		Promise<Void> extraAcknowledge = supplier.apply(queue.getSupplier());
		ChannelConsumer<T> result = queue.getConsumer();
		if (extraAcknowledge == Promise.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	/**
	 * Unwraps {@code ChannelConsumer} of provided {@code Promise}.
	 * If provided Promise is already successfully completed, its
	 * result will be returned, otherwise an {@code AbstractChannelConsumer}
	 * is created, which waits for the Promise to be completed before accepting
	 * any value. A Promise of Exception will be returned if Promise was completed
	 * with an exception.
	 *
	 * @param promise Promise of {@code ChannelConsumer}
	 * @param <T>     type of data to be consumed
	 * @return ChannelConsumer b
	 */
	static <T> ChannelConsumer<T> ofPromise(Promise<? extends ChannelConsumer<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new AbstractChannelConsumer<T>() {
			ChannelConsumer<T> consumer;
			Throwable exception;

			@Override
			protected Promise<Void> doAccept(T value) {
				if (consumer != null) return consumer.accept(value);
				return promise.thenEx((consumer, e) -> {
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
			protected void onClosed(@NotNull Throwable e) {
				exception = e;
				promise.whenResult(supplier -> supplier.close(e));
			}
		};
	}

	/**
	 * Returns a {@code ChannelConsumer} wrapped in {@link Supplier}
	 * and calls its {@code accept()} when {@code accept()} method is called.
	 *
	 * @param provider provider of the {@code ChannelConsumer}
	 * @return a {@code ChannelConsumer} which was wrapped in the {@code provider}
	 */
	static <T> ChannelConsumer<T> ofLazyProvider(Supplier<? extends ChannelConsumer<T>> provider) {
		return new AbstractChannelConsumer<T>() {
			private ChannelConsumer<T> consumer;

			@Override
			protected Promise<Void> doAccept(@Nullable T value) {
				if (consumer == null) consumer = provider.get();
				return consumer.accept(value);
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				if (consumer != null) {
					consumer.close(e);
				}
			}
		};
	}

	/**
	 * Wraps {@link AsyncTcpSocket#write(ByteBuf)} operation into {@link ChannelConsumer}.
	 *
	 * @return {@link ChannelConsumer} of ByteBufs that will be sent to network
	 */
	static ChannelConsumer<ByteBuf> ofSocket(AsyncTcpSocket socket) {
		return ChannelConsumer.of(socket::write, socket)
				.withAcknowledgement(ack -> ack
						.then($ -> socket.write(null)));
	}

	/**
	 * Transforms current {@code ChannelConsumer} with provided {@link ChannelConsumerTransformer}.
	 *
	 * @param fn  transformer of the {@code ChannelConsumer}
	 * @param <R> result value after transformation
	 * @return result of transformation applied to the current {@code ChannelConsumer}
	 */
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

	/**
	 * Creates a wrapper ChannelConsumer which executes current
	 * ChannelConsumer's {@code accept(T value)} within the
	 * {@code asyncExecutor}.
	 *
	 * @param asyncExecutor executes ChannelConsumer
	 * @return a wrapper of current ChannelConsumer which executes
	 * in provided {@code asyncExecutor}
	 */
	default ChannelConsumer<T> withExecutor(AsyncExecutor asyncExecutor) {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return asyncExecutor.execute(() -> ChannelConsumer.this.accept(value));
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, if provided {@code value} doesn't equal {@code null}, it
	 * will be accepted by the provided {@code fn} first and then by this
	 * ChannelConsumer.
	 *
	 * @param fn {@link Consumer} which accepts the value passed by {@code apply(T value)}
	 * @return a wrapper ChannelConsumer
	 */
	default ChannelConsumer<T> peek(Consumer<? super T> fn) {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) fn.accept(value);
				return ChannelConsumer.this.accept(value);
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, {@code fn} will be applied to the provided {@code value} first
	 * and the result of the {@code fn} will be accepted by current ChannelConsumer.
	 * If provide {@code value} is {@code null}, {@code fn} won't be applied.
	 *
	 * @param fn  {@link Function} to be applied to the value of {@code apply(T value)}
	 * @param <V> type of data accepted and returned by the {@code fn} and accepted by ChannelConsumer
	 * @return a wrapper ChannelConsumer
	 */
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

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, {@code fn} will be applied to the provided {@code value} first
	 * and the result of the {@code fn} will be accepted by current ChannelConsumer
	 * asynchronously. If provided {@code value} is {@code null}, {@code fn} won't
	 * be applied.
	 *
	 * @param fn  {@link Function} to be applied to the value of {@code apply(T value)}
	 * @param <V> type of data accepted by the {@code fn} and ChannelConsumer
	 * @return a wrapper ChannelConsumer
	 */
	default <V> ChannelConsumer<V> mapAsync(Function<? super V, ? extends Promise<T>> fn) {
		return new AbstractChannelConsumer<V>(this) {
			@Override
			protected Promise<Void> doAccept(V value) {
				return value != null ?
						fn.apply(value)
								.then(ChannelConsumer.this::accept) :
						ChannelConsumer.this.accept(null);
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, current ChannelConsumer will accept the value only of it
	 * passes {@link Predicate} test.
	 *
	 * @param predicate {@link Predicate} which is used to filter accepted value
	 * @return a wrapper ChannelConsumer
	 */
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

	/**
	 * Creates a wrapper ChannelConsumer - after its {@code accept(T value)}
	 * is called and completed, an acknowledgement is returned. An acknowledgement
	 * is a {@link SettablePromise} which is accepted by the provided {@code fn}
	 * and then materialized.
	 *
	 * @param fn a function applied to the {@code SettablePromise} which is then
	 *           materialized and returned
	 * @return a wrapper ChannelConsumer
	 */
	default ChannelConsumer<T> withAcknowledgement(Function<Promise<Void>, Promise<Void>> fn) {
		SettablePromise<Void> acknowledgement = new SettablePromise<>();
		Promise<Void> newAcknowledgement = fn.apply(acknowledgement);
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(@Nullable T value) {
				if (value != null) {
					return ChannelConsumer.this.accept(value)
							.thenEx(($, e) -> {
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
			protected void onClosed(@NotNull Throwable e) {
				acknowledgement.trySetException(e);
			}
		};
	}

	/**
	 * Returns a Promise as a marker of completion.
	 */
	static Promise<Void> getAcknowledgement(Consumer<Function<Promise<Void>, Promise<Void>>> fn) {
		return Promise.ofCallback(cb ->
				fn.accept(ack -> ack.whenComplete(cb)));
	}

}
