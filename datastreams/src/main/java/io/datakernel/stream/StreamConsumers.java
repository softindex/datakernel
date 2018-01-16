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

package io.datakernel.stream;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.async.Stages.onError;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final Throwable exception;
		private final SettableStage<Void> endOfStream = SettableStage.create();

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setProducer(StreamProducer<T> producer) {
			getCurrentEventloop().post(() -> endOfStream.trySetException(exception));
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	static final class IdleImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		@Override
		public void setProducer(StreamProducer<T> producer) {
			producer.getEndOfStream()
					.whenComplete(Stages.onResult(endOfStream::trySet))
					.whenComplete(onError(endOfStream::trySetException));
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
		}
	}

	public static <T> StreamConsumer<T> errorDecorator(StreamConsumer<T> consumer, Predicate<T> predicate, Supplier<Throwable> error) {
		return new StreamConsumerDecorator<T>(consumer) {
			@Override
			protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
				return super.onProduce(item -> {
					if (predicate.test(item)) {
						this.closeWithError(error.get());
					} else {
						dataReceiver.onData(item);
					}
				});
			}
		};
	}

	public static <T, R> StreamConsumerWithResult<T, R> errorDecorator(StreamConsumerWithResult<T, R> consumer, Predicate<T> predicate, Supplier<Throwable> error) {
		return errorDecorator((StreamConsumer<T>) consumer, predicate, error).withResult(consumer.getResult());
	}

	public static <T> StreamConsumer<T> suspendDecorator(StreamConsumer<T> consumer,
	                                                     Predicate<T> predicate,
	                                                     BiConsumer<StreamProducer<T>, StreamDataReceiver<T>> resumer) {
		return new StreamConsumerDecorator<T>(consumer) {
			@Override
			protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
				StreamProducer<T> producer = this.getProducer();
				return super.onProduce(new StreamDataReceiver<T>() {
					@Override
					public void onData(T item) {
						dataReceiver.onData(item);

						if (predicate.test(item)) {
							producer.suspend();
							resumer.accept(producer, this);
						}
					}
				});
			}
		};
	}

	public static <T, R> StreamConsumerWithResult<T, R> suspendDecorator(StreamConsumerWithResult<T, R> consumer,
	                                                                     Predicate<T> predicate,
	                                                                     BiConsumer<StreamProducer<T>, StreamDataReceiver<T>> resumer) {
		return suspendDecorator((StreamConsumer<T>) consumer, predicate, resumer).withResult(consumer.getResult());
	}

}
