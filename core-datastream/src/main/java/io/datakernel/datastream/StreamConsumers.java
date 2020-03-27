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
import io.datakernel.datastream.visitor.StreamVisitor;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collector;

import static io.datakernel.common.Utils.nullify;

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

	static final class ClosingWithError<T> extends AbstractStreamConsumer<T> {
		private Throwable error;

		ClosingWithError(Throwable e) {
			this.error = e;
		}

		@Override
		protected void onInit() {
			error = nullify(error, this::closeEx);
		}
	}

	static final class OfPromise<T> extends AbstractStreamConsumer<T> {
		private Promise<? extends StreamConsumer<T>> promise;
		private final InternalSupplier internalSupplier = new InternalSupplier();

		private class InternalSupplier extends AbstractStreamSupplier<T> {
			@Override
			protected void onResumed() {
				OfPromise.this.resume(getDataAcceptor());
			}

			@Override
			protected void onSuspended() {
				OfPromise.this.suspend();
			}

			@Override
			public void accept(StreamVisitor visitor) {
				super.accept(visitor);
				visitor.visitImplicit(OfPromise.this, this);
				if (visitor.unseen(OfPromise.this)) {
					OfPromise.this.accept(visitor);
				}
			}
		}

		public OfPromise(@NotNull Promise<? extends StreamConsumer<T>> promise) {
			this.promise = promise;
		}

		@Override
		protected void onInit() {
			promise
					.whenResult(consumer -> {
						consumer.getAcknowledgement()
								.whenResult(this::acknowledge)
								.whenException(this::closeEx);
						getAcknowledgement()
								.whenException(consumer::closeEx);
						internalSupplier.streamTo(consumer);
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onEndOfStream() {
			internalSupplier.sendEndOfStream();
		}

		@Override
		protected void onCleanup() {
			promise = null;
		}

		@Override
		public void accept(StreamVisitor visitor) {
			super.accept(visitor);
			if (visitor.unseen(internalSupplier)) {
				internalSupplier.accept(visitor);
			}
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
		}

		@Override
		protected void onInit() {
			queue.getSupplier().streamTo(consumer)
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onStarted() {
			flush();
		}

		private void flush() {
			resume(item -> {
				Promise<Void> promise = queue.put(item);
				if (promise.isComplete()) return;
				suspend();
				promise.whenResult(() -> {
					if (!isEndOfStream()) {
						flush();
					} else {
						sendEndOfStream();
					}
				});
			});
		}

		@Override
		protected void onEndOfStream() {
			// end of stream is sent either from here or from queues waiting put promise
			// callback, but not from both and this condition ensures that
			if (!queue.isWaitingPut()) {
				sendEndOfStream();
			}
		}

		private void sendEndOfStream() {
			queue.put(null);
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

		public ToCollector(Collector<T, A, R> collector) {
			this.collector = collector;
		}

		public Promise<R> getResult() {
			return resultPromise;
		}

		@Override
		protected void onInit() {
			resultPromise.whenComplete(this::acknowledge);
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
			resultPromise.set(collector.finisher().apply(accumulator));
		}

		@Override
		protected void onError(Throwable e) {
			resultPromise.setException(e);
		}

		@Override
		protected void onComplete() {
			accumulator = null;
		}

	}
}
