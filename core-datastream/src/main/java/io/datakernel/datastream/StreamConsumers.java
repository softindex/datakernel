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

package io.datakernel.datastream;

import io.datakernel.common.exception.UncheckedException;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collector;

import static io.datakernel.common.Preconditions.checkState;

final class StreamConsumers {

	static final class ClosingWithError<T> implements StreamConsumer<T> {
		private final Promise<Void> acknowledgement;

		ClosingWithError(Throwable e) {
			this.acknowledgement = Promise.ofException(e);
		}

		@Override
		public void consume(@NotNull StreamSupplier<T> streamSupplier) {
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
		}
	}

	static final class OfConsumer<T> extends AbstractStreamConsumer<T> {
		private final Consumer<T> consumer;

		OfConsumer(Consumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			resume(item -> {
				try {
					consumer.accept(item);
				} catch (UncheckedException u) {
					closeEx(u.getCause());
				}
			});
		}

		@Override
		protected void onEndOfStream() {
			acknowledge();
		}
	}

	static final class Idle<T> extends AbstractStreamConsumer<T> {
		@Override
		protected void onEndOfStream() {
			acknowledge();
		}
	}

	static final class Skip<T> extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			resume(item -> {});
		}

		@Override
		protected void onEndOfStream() {
			acknowledge();
		}
	}

	static final class OfPromise<T> extends AbstractStreamConsumer<T> {
		final @NotNull Promise<? extends StreamConsumer<T>> promise;

		public OfPromise(@NotNull Promise<? extends StreamConsumer<T>> promise) {
			this.promise = promise;
		}

		@Override
		protected void onStarted() {
			promise
					.whenResult(consumer -> {
						consumer.getAcknowledgement()
								.whenComplete(this::acknowledge)
								.whenException(this::closeEx);
						this.getAcknowledgement()
								.whenException(consumer::closeEx);
						if (isClosed()) return;
						consumer.consume(supplier);
					})
					.whenException(this::closeEx);
		}
	}

	static final class OfChannelConsumer<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private final ChannelQueue<T> queue;
		private final ChannelConsumer<T> consumer;

		OfChannelConsumer(ChannelConsumer<T> consumer) {
			this(new ChannelZeroBuffer<>(), consumer);
		}

		OfChannelConsumer(ChannelQueue<T> queue, ChannelConsumer<T> consumer) {
			this.queue = queue;
			this.consumer = consumer;
			queue.getSupplier().streamTo(consumer)
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onStarted() {
			resume(this);
		}

		@Override
		protected void onEndOfStream() {
			if (!queue.isWaitingPut()) {
				queue.put(null);
			}
		}

		@Override
		public void accept(T item) {
			assert item != null;
			Promise<Void> promise = queue.put(item);
			if (!promise.isComplete()) {
				suspend();
				promise.whenResult(() -> { // *
					if (isClosed()) return;
					if (!isEndOfStream()) {
						this.resume(this);
					} else {
						onEndOfStream();
					}
				});
			}
		}

		@Override
		protected void onError(Throwable e) {
			consumer.closeEx(e);
		}
	}

	static final class AsChannelConsumer<T> extends AbstractChannelConsumer<T> {
		private final StreamConsumer<T> streamConsumer;
		private StreamDataAcceptor<T> dataAcceptor;
		private T item;
		private SettablePromise<Void> itemPromise;
		final SettablePromise<Void> endOfStream = new SettablePromise<>();

		AsChannelConsumer(StreamConsumer<T> consumer) {
			streamConsumer = consumer;
			streamConsumer.getAcknowledgement()
					.whenResult(this::close)
					.whenException(this::closeEx);
			if (!streamConsumer.getAcknowledgement().isComplete()) {
				streamConsumer.consume(new StreamSupplier<T>() {
					@Override
					public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
						if (isClosed()) return;
						if (streamConsumer.getAcknowledgement().isComplete()) return;
						AsChannelConsumer.this.dataAcceptor = dataAcceptor;
						if (dataAcceptor != null) {
							if (item != null) {
								dataAcceptor.accept(item);
								itemPromise.set(null);
							}
						}
					}

					@Override
					public Promise<Void> getEndOfStream() {
						return endOfStream;
					}

					@Override
					public void closeEx(@NotNull Throwable e) {
					}
				});
			}
		}

		@Override
		protected Promise<Void> doAccept(@Nullable T item) {
			assert !isClosed();
			assert !streamConsumer.getAcknowledgement().isComplete();
			if (item == null) {
				endOfStream.trySet(null);
				return streamConsumer.getAcknowledgement();
			}
			if (dataAcceptor != null) {
				dataAcceptor.accept(item);
				return Promise.complete();
			} else {
				this.item = item;
				this.itemPromise = new SettablePromise<>();
				this.itemPromise.whenComplete(() -> { // *
					this.item = null;
					this.itemPromise = null;
				});
				return this.itemPromise;
			}
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			dataAcceptor = null;
			endOfStream.trySetException(e);
			streamConsumer.closeEx(e);
			if (itemPromise != null) {
				itemPromise.setException(e); // *
			}
			checkState(this.dataAcceptor == null);
			checkState(this.item == null);
		}
	}

	static final class ToCollector<T, A, R> extends AbstractStreamConsumer<T> {
		private final Collector<T, A, R> collector;
		private final SettablePromise<R> resultPromise = new SettablePromise<>();
		@Nullable
		private A accumulator;

		{
			resultPromise.whenComplete(this::acknowledge);
			acknowledgement.whenException(resultPromise::trySetException);
		}

		public ToCollector(Collector<T, A, R> collector) {
			this.collector = collector;
		}

		@Override
		protected void onStarted() {
			A accumulator = collector.supplier().get();
			this.accumulator = accumulator;
			BiConsumer<A, T> consumer = collector.accumulator();
			resume(item -> consumer.accept(accumulator, item));
		}

		@Override
		protected void onEndOfStream() {
			if (resultPromise.isComplete()) return;
			R result = collector.finisher().apply(accumulator);
			accumulator = null;
			resultPromise.set(result);
		}

		public Promise<R> getResult() {
			return resultPromise;
		}

		@Nullable
		public A getAccumulator() {
			return accumulator;
		}

	}
}
