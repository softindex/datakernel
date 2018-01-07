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

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.async.SettableStage.mirrorOf;
import static io.datakernel.async.Stages.onError;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.DataStreams.stream;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	public static <T> IdleImpl<T> idle() {
		return new IdleImpl<>();
	}

	public static <T> StreamConsumer<T> closingWithError(Throwable exception) {
		return new ClosingWithErrorImpl<>(exception);
	}

	public static <T> StreamConsumer<T> ofStage(CompletionStage<StreamConsumer<T>> consumerStage) {
		return new StreamConsumerDecorator<T>() {
			{
				consumerStage.whenComplete((consumer, throwable) -> {
					if (throwable == null) {
						setActualConsumer(consumer);
					} else {
						setActualConsumer(closingWithError(throwable));
					}
				});
			}
		};
	}

	public static <T, X> StreamConsumerWithResult<T, X> ofStageWithResult(CompletionStage<StreamConsumerWithResult<T, X>> stage) {
		SettableStage<X> result = SettableStage.create();
		return withResult(new StreamConsumerDecorator<T>() {
			{
				stage.whenComplete((consumer, throwable) -> {
					if (throwable == null) {
						setActualConsumer(consumer);
						consumer.getResult().whenComplete(result::set);
					} else {
						setActualConsumer(closingWithError(throwable));
						result.setException(throwable);
					}
				});
			}
		}, result);
	}

	public static <T> StreamConsumerWithResult<T, Void> withEndOfStreamAsResult(StreamConsumer<T> consumer) {
		return new StreamConsumerWithResult<T, Void>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				consumer.setProducer(producer);
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return consumer.getEndOfStream();
			}

			@Override
			public CompletionStage<Void> getResult() {
				return consumer.getEndOfStream();
			}
		};
	}

	public static <T, X> StreamConsumerWithResult<T, X> withResult(StreamConsumer<T> consumer, CompletionStage<X> result) {
		SettableStage<X> safeResult = mirrorOf(result);
		consumer.getEndOfStream().whenComplete(onError(safeResult::trySetException));
		return new StreamConsumerWithResult<T, X>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				consumer.setProducer(producer);
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return consumer.getEndOfStream();
			}

			@Override
			public CompletionStage<X> getResult() {
				return safeResult;
			}
		};
	}

	/**
	 * Returns {@link StreamConsumerToList} which saves received items in empty list
	 *
	 * @param <T> type of item
	 */
	public static <T> StreamConsumerWithResult<T, List<T>> toList() {
		return new StreamConsumerToList<>();
	}

	public static <T> CompletionStage<List<T>> toList(StreamProducer<T> streamProducer) {
		return stream(streamProducer, new StreamConsumerToList<>());
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
		return withResult(errorDecorator((StreamConsumer<T>) consumer, predicate, error), consumer.getResult());
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
		return withResult(suspendDecorator((StreamConsumer<T>) consumer, predicate, resumer), consumer.getResult());
	}

}
