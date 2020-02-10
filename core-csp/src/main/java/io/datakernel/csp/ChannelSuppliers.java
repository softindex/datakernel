/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.async.process.Cancellable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import io.datakernel.common.collection.CollectionUtils;
import io.datakernel.common.collection.Try;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.csp.queue.ChannelBuffer;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.common.Recyclable.deepRecycle;
import static io.datakernel.common.Recyclable.tryRecycle;
import static io.datakernel.common.Utils.nullify;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.lang.Math.min;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

/**
 * Provides additional functionality for managing {@link ChannelSupplier}s.
 * Includes helper classes: ChannelSupplierOfException, ChannelSupplierOfIterator,
 * ChannelSupplierOfValue, ChannelSupplierEmpty.
 */
public final class ChannelSuppliers {

	/**
	 * @see #concat(Iterator)
	 */
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T> supplier1, ChannelSupplier<? extends T> supplier2) {
		return concat(CollectionUtils.asIterator(supplier1, supplier2));
	}

	/**
	 * @see #concat(Iterator)
	 */
	@SafeVarargs
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T>... suppliers) {
		return concat(CollectionUtils.asIterator(suppliers));
	}

	/**
	 * Creates a new ChannelSupplier which on {@code get()} call returns
	 * the result wrapped in {@code promise} of the first ChannelSuppliers'
	 * {@code promise} that was successfully completed with a non-null result.
	 * If all of the ChannelSuppliers of the iterator have a {@code null}
	 * {@code promise} result, a {@code promise} of {@code null} will be returned.
	 * <p>
	 * If one of the ChannelSuppliers' {@code promises} completes with an exception,
	 * all subsequent elements of the iterator will be closed and a
	 * {@code promise} of exception will be returned.
	 *
	 * @param iterator an iterator of ChannelSuppliers
	 * @param <T>      type of data wrapped in the ChannelSuppliers
	 * @return a ChannelSupplier of {@code <T>}
	 */
	public static <T> ChannelSupplier<T> concat(Iterator<? extends ChannelSupplier<? extends T>> iterator) {
		return new AbstractChannelSupplier<T>() {
			ChannelSupplier<? extends T> current = ChannelSupplier.of();

			@Override
			protected Promise<T> doGet() {
				return current.get()
						.thenEx((value, e) -> {
							if (e == null) {
								if (value != null) {
									return Promise.of(value);
								} else {
									if (iterator.hasNext()) {
										current = iterator.next();
										return get();
									} else {
										return Promise.of(null);
									}
								}
							} else {
								while (iterator.hasNext()) {
									iterator.next().close(e);
								}
								return Promise.ofException(e);
							}
						});
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				current.close(e);
				while (iterator.hasNext()) {
					iterator.next().close(e);
				}
			}
		};
	}

	/**
	 * Collects data provided by the {@code supplier} asynchronously and returns a
	 * promise of accumulated result. This process will be getting values from the
	 * {@code supplier}, until a promise of {@code null} is returned, which represents
	 * end of stream.
	 * <p>
	 * If {@code get} returns a promise of exception or there was an exception while
	 * {@code accumulator} accepted values, a promise of {@code exception} will be
	 * returned and the process will stop.
	 *
	 * @param supplier     a {@code ChannelSupplier} which provides data to be collected
	 * @param initialValue a value which will accumulate the results of accumulator
	 * @param accumulator  a {@link BiConsumer} which may perform some operations over provided
	 *                     by supplier data and accumulates the result to the initialValue
	 * @param finisher     a {@link Function} which performs the final transformation of the
	 *                     accumulated value
	 * @param <T>          a data type provided by the {@code supplier}
	 * @param <A>          an intermediate accumulation data type
	 * @param <R>          a data type of final result of {@code finisher}
	 * @return a promise of accumulated result, transformed by the {@code finisher}
	 */
	public static <T, A, R> Promise<R> collect(ChannelSupplier<T> supplier,
			A initialValue, BiConsumer<A, T> accumulator, Function<A, R> finisher) {
		return Promise.ofCallback(cb ->
				toCollectorImpl(supplier, initialValue, accumulator, finisher, cb));
	}

	private static <T, A, R> void toCollectorImpl(ChannelSupplier<T> supplier,
			A accumulatedValue, BiConsumer<A, T> accumulator, Function<A, R> finisher,
			SettablePromise<R> cb) {
		Promise<T> promise;
		while (true) {
			promise = supplier.get();
			if (!promise.isResult()) break;
			T item = promise.getResult();
			if (item != null) {
				try {
					accumulator.accept(accumulatedValue, item);
				} catch (UncheckedException u) {
					Throwable cause = u.getCause();
					supplier.close(cause);
					cb.setException(cause);
					return;
				}
				continue;
			}
			break;
		}
		promise.whenComplete((value, e) -> {
			if (e == null) {
				if (value != null) {
					try {
						accumulator.accept(accumulatedValue, value);
					} catch (UncheckedException u) {
						Throwable cause = u.getCause();
						supplier.close(cause);
						cb.setException(cause);
						return;
					}
					toCollectorImpl(supplier, accumulatedValue, accumulator, finisher, cb);
				} else {
					cb.set(finisher.apply(accumulatedValue));
				}
			} else {
				deepRecycle(finisher.apply(accumulatedValue));
				cb.setException(e);
			}
		});
	}

	public static <T> Promise<Void> streamTo(Promise<ChannelSupplier<T>> supplier, Promise<ChannelConsumer<T>> consumer) {
		return Promises.toTuple(supplier.toTry(), consumer.toTry())
				.then(t -> streamTo(t.getValue1(), t.getValue2()));
	}

	public static <T> Promise<Void> streamTo(Try<ChannelSupplier<T>> supplier, Try<ChannelConsumer<T>> consumer) {
		if (supplier.isSuccess() && consumer.isSuccess()) {
			return streamTo(supplier.get(), consumer.get());
		}
		StacklessException exception = new StacklessException("Channel stream failed");
		supplier.consume(Cancellable::cancel, exception::addSuppressed);
		consumer.consume(Cancellable::cancel, exception::addSuppressed);
		return Promise.ofException(exception);
	}

	/**
	 * Streams data from the {@code supplier} to the {@code consumer} until {@code get()}
	 * of {@code supplier} returns a promise of {@code null}.
	 * <p>
	 * If {@code get} returns a promise of exception or there was an exception while
	 * {@code consumer} accepted values, a promise of {@code exception} will be
	 * returned and the process will stop.
	 *
	 * @param supplier a supplier which provides some data
	 * @param consumer a consumer which accepts the provided by supplier data
	 * @param <T>      a data type of values passed from the supplier to consumer
	 * @return a promise of {@code null} as a marker of completion of stream,
	 * or promise of exception, if there was an exception while streaming
	 */
	public static <T> Promise<Void> streamTo(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer) {
		return Promise.ofCallback(cb ->
				streamToImpl(supplier, consumer, cb));
	}

	private static <T> void streamToImpl(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer, SettablePromise<Void> cb) {
		Promise<T> supplierPromise;
		while (true) {
			supplierPromise = supplier.get();
			if (!supplierPromise.isResult()) break;
			T item = supplierPromise.getResult();
			if (item == null) break;
			Promise<Void> consumerPromise = consumer.accept(item);
			if (consumerPromise.isResult()) continue;
			consumerPromise.whenComplete(($, e) -> {
				if (e == null) {
					streamToImpl(supplier, consumer, cb);
				} else {
					supplier.close(e);
					cb.trySetException(e);
				}
			});
			return;
		}
		supplierPromise
				.whenComplete((item, e1) -> {
					if (e1 == null) {
						consumer.accept(item)
								.whenComplete(($, e2) -> {
									if (e2 == null) {
										if (item != null) {
											streamToImpl(supplier, consumer, cb);
										} else {
											cb.trySet(null);
										}
									} else {
										supplier.close(e2);
										cb.trySetException(e2);
									}
								});
					} else {
						consumer.close(e1);
						cb.trySetException(e1);
					}
				});
	}

	public static <T> ChannelSupplier<T> prefetch(int count, ChannelSupplier<T> actual) {
		ChannelBuffer<T> buffer = new ChannelBuffer<>(count);
		actual.streamTo(buffer.getConsumer());
		return buffer.getSupplier();
	}

	public static <T> ChannelSupplier<T> prefetch(ChannelSupplier<T> actual) {
		ChannelZeroBuffer<T> buffer = new ChannelZeroBuffer<>();
		actual.streamTo(buffer.getConsumer());
		return buffer.getSupplier();
	}

	/**
	 * Transforms this {@code ChannelSupplier} data of <T> type with provided {@code fn},
	 * which returns an {@link Iterator} of a <V> type. Then provides this value to ChannelSupplier of <V>.
	 */
	public static <T, V> ChannelSupplier<V> remap(ChannelSupplier<T> supplier, Function<? super T, ? extends Iterator<? extends V>> fn) {
		return new AbstractChannelSupplier<V>(supplier) {
			Iterator<? extends V> iterator = CollectionUtils.emptyIterator();
			boolean endOfStream;

			@Override
			protected Promise<V> doGet() {
				if (iterator.hasNext()) return Promise.of(iterator.next());
				return Promise.ofCallback(this::next);
			}

			private void next(SettablePromise<V> cb) {
				if (!endOfStream) {
					supplier.get()
							.whenComplete((item, e) -> {
								if (e == null) {
									if (item == null) endOfStream = true;
									iterator = fn.apply(item);
									if (iterator.hasNext()) {
										cb.set(iterator.next());
									} else {
										next(cb);
									}
								} else {
									cb.setException(e);
								}
							});
				} else {
					cb.set(null);
				}
			}
		};
	}

	public static ChannelSupplier<ByteBuf> inputStreamAsChannelSupplier(Executor executor, MemSize bufSize, InputStream is) {
		return inputStreamAsChannelSupplier(executor, bufSize.toInt(), is);
	}

	public static ChannelSupplier<ByteBuf> inputStreamAsChannelSupplier(Executor executor, int bufSize, InputStream inputStream) {
		return new AbstractChannelSupplier<ByteBuf>() {
			@Override
			protected Promise<ByteBuf> doGet() {
				return Promise.ofBlockingCallable(executor, () -> {
					ByteBuf buf = ByteBufPool.allocate(bufSize);
					int readBytes;
					try {
						readBytes = inputStream.read(buf.array(), 0, bufSize);
					} catch (IOException e) {
						throw new UncheckedException(e);
					}
					if (readBytes != -1) {
						buf.moveTail(readBytes);
						return buf;
					} else {
						buf.recycle();
						return null;
					}
				});
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				executor.execute(() -> {
					try {
						inputStream.close();
					} catch (IOException ignored) {
					}
				});
			}
		};
	}

	public static InputStream channelSupplierAsInputStream(Eventloop eventloop, ChannelSupplier<ByteBuf> channelSupplier) {
		return channelSupplierAsInputStream(eventloop, channelSupplier, Long.MAX_VALUE);
	}

	public static InputStream channelSupplierAsInputStream(Eventloop eventloop, ChannelSupplier<ByteBuf> channelSupplier, long miliseconds) {
		return new InputStream() {
			@Nullable ByteBuf current = null;
			private boolean isClosed;

			@Override
			public int read() throws IOException {
				return doRead(ByteBuf::readByte);
			}

			@Override
			public int read(@NotNull byte[] b, int off, int len) throws IOException {
				return doRead(buf -> buf.read(b, off, min(len, buf.readRemaining())));
			}

			@Override
			public int available() {
				return current != null && !isClosed ? current.readRemaining() : 0;
			}

			private int doRead(ToIntFunction<ByteBuf> reader) throws IOException {
				checkState(!eventloop.inEventloopThread(), "In eventloop thread");
				if (isClosed) return -1;
				ByteBuf peeked = current;
				if (peeked == null) {
					ByteBuf buf = null;
					do {
						CompletableFuture<ByteBuf> future = eventloop.submit(channelSupplier::get);
						try {
							buf = future.get(miliseconds, MILLISECONDS);
						} catch (InterruptedException e) {
							isClosed = true;
							eventloop.submit(() -> channelSupplier.close(e));
							throw new IOException(e);
						} catch (ExecutionException e) {
							handleException(e);
						} catch (TimeoutException e) {
							return -1; // cannot fetch more data
						}
						if (buf == null) {
							isClosed = true;
							return -1;
						}
					} while (!buf.canRead());
					peeked = buf;
				}
				int result = reader.applyAsInt(peeked);
				if (peeked.canRead()) {
					current = peeked;
				} else {
					current = null;
					peeked.recycle();
				}
				return result;
			}

			@Override
			public void close() throws IOException {
				checkState(!eventloop.inEventloopThread(), "In eventloop thread");
				if (!isClosed) {
					isClosed = true;
					CompletableFuture<Void> close = eventloop.submit(() -> channelSupplier.streamTo(ChannelConsumers.recycling()));
					try {
						close.get();
					} catch (InterruptedException e) {
						throw new IOException(e);
					} catch (ExecutionException e) {
						handleException(e);
					}
				} else {
					doClose();
				}
			}

			private void handleException(Throwable e) throws IOException {
				isClosed = true;
				Throwable cause = e.getCause();
				if (cause instanceof IOException) throw (IOException) cause;
				if (cause instanceof RuntimeException) throw (RuntimeException) cause;
				if (cause instanceof Exception) throw new IOException(cause);
				throw (Error) cause;
			}

			private void doClose() {
				current = nullify(current, ByteBuf::recycle);
				eventloop.submit((Runnable) channelSupplier::close);
			}
		};
	}

	/**
	 * Represents a {@code ChannelSupplier} which always returns
	 * a promise of {@code null}.
	 */
	public static class ChannelSupplierEmpty<T> extends AbstractChannelSupplier<T> {
		@Override
		protected Promise<T> doGet() {
			return Promise.of(null);
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} of one value. Returns a promise of the value when
	 * {@code get} is called for the first time, all subsequent calls will return {@code null}.
	 */
	public static final class ChannelSupplierOfValue<T> extends AbstractChannelSupplier<T> {
		private T item;

		public ChannelSupplierOfValue(@NotNull T item) {
			this.item = item;
		}

		public T getValue() {
			return item;
		}

		public T takeValue() {
			T item = this.item;
			this.item = null;
			return item;
		}

		@Override
		protected Promise<T> doGet() {
			T item = takeValue();
			return Promise.of(item);
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			tryRecycle(item);
			item = null;
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} which wraps the provided iterator and
	 * returns promises of iterator's values until {@code hasNext()} is true, when
	 * there are no more values left, a promise of {@code null} is returned.
	 */
	public static final class ChannelSupplierOfIterator<T> extends AbstractChannelSupplier<T> {
		private final Iterator<? extends T> iterator;

		public ChannelSupplierOfIterator(Iterator<? extends T> iterator) {
			this.iterator = iterator;
		}

		@Override
		protected Promise<T> doGet() {
			return Promise.of(iterator.hasNext() ? iterator.next() : null);
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			deepRecycle(iterator);
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} which always returns a promise of exception.
	 */
	public static final class ChannelSupplierOfException<T> extends AbstractChannelSupplier<T> {
		private final Throwable e;

		public ChannelSupplierOfException(Throwable e) {
			this.e = e;
		}

		@Override
		protected Promise<T> doGet() {
			return Promise.ofException(e);
		}
	}
}
