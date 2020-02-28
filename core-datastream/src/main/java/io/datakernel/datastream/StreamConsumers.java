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

final class StreamConsumers {

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

	static final class OfPromise<T> extends AbstractStreamConsumer<T> {
		@Nullable
		private StreamConsumer<T> consumer;

		public OfPromise(@NotNull Promise<? extends StreamConsumer<T>> promise) {
			promise
					.whenResult(consumer -> {
						consumer.getAcknowledgement()
								.whenResult(this::acknowledge)
								.whenException(this::closeEx);
						if (isClosed()) return;

						this.consumer = consumer;
						if (supplier != null) {
							consumer.consume(supplier);
						}
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onStarted() {
			assert supplier != null;
			if (consumer != null) {
				consumer.consume(supplier);
			}
		}

		@Override
		protected void onError(Throwable e) {
			if (consumer != null) {
				consumer.closeEx(e);
			}
		}

		@Override
		protected void onCleanup() {
			consumer = null;
		}
	}

	static final class OfChannelConsumer<T> extends AbstractStreamConsumer<T> {
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

		private void sendEndOfStream() {
			queue.put(null);
		}

		private void resume() {
			resume(item -> {
				Promise<Void> promise = queue.put(item);
				if (promise.isComplete()) return;
				suspend();
				promise.whenResult(() -> {
					if (isClosed()) return;
					if (!isEndOfStream()) {
						resume();
					} else {
						sendEndOfStream();
					}
				});
			});
		}

		@Override
		protected void onStarted() {
			resume();
		}

		@Override
		protected void onEndOfStream() {
			// end of stream is sent either from here or from queues waiting put promise
			// callback, but not from both and this condition ensures that
			if (!queue.isWaitingPut()) {
				sendEndOfStream();
			}
		}

		@Override
		protected void onError(Throwable e) {
			consumer.closeEx(e);
		}
	}

	static final class ToCollector<T, A, R> extends AbstractStreamConsumer<T> {
		private final SettablePromise<R> resultPromise = new SettablePromise<>();
		private Collector<T, A, R> collector;
		private A accumulator;

		{
			resultPromise.whenComplete(this::acknowledge);
		}

		public ToCollector(Collector<T, A, R> collector) {
			this.collector = collector;
			accumulator = collector.supplier().get();
		}

		public Promise<R> getResult() {
			return resultPromise;
		}

		@Override
		protected void onStarted() {
			BiConsumer<A, T> consumer = collector.accumulator();
			resume(item -> consumer.accept(accumulator, item));
		}

		@Override
		protected void onEndOfStream() {
			resultPromise.set(collector.finisher().apply(accumulator));
		}

		@Override
		protected void onError(Throwable e) {
			resultPromise.setException(e);
		}

		@Override
		protected void onCleanup() {
			accumulator = null;
		}

	}
}
