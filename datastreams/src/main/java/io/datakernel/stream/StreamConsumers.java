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
import io.datakernel.eventloop.Eventloop;

import java.util.List;
import java.util.concurrent.CompletionStage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

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
		StreamConsumerDecorator<T, Void> decorator = new StreamConsumerDecorator<T, Void>() {};
		consumerStage.whenComplete((consumer, throwable) -> {
			if (throwable == null) {
				decorator.setActualConsumer(consumer);
			} else {
				decorator.setActualConsumer(closingWithError(throwable));
			}
		});
		return decorator;
	}

	public static <T, X> StreamConsumerWithResult<T, X> ofStageWithResult(CompletionStage<StreamConsumerWithResult<T, X>> stage) {
		StreamConsumerDecorator<T, X> decorator = new StreamConsumerDecorator<T, X>() {};
		stage.whenComplete((consumer, throwable) -> {
			if (throwable == null) {
				decorator.setActualConsumer(consumer, consumer.getResult());
			} else {
				decorator.setActualConsumer(closingWithError(throwable));
			}
		});
		return decorator;
	}

	public static <T> StreamConsumerWithResult<T, Void> withEndOfStream(StreamConsumer<T> consumer) {
		if (consumer instanceof HasEndOfStream) {
			return new StreamConsumerWithResult<T, Void>() {
				@Override
				public void setProducer(StreamProducer<T> producer) {
					consumer.setProducer(producer);
				}

				@Override
				public void endOfStream() {
					consumer.endOfStream();
				}

				@Override
				public void closeWithError(Throwable t) {
					consumer.closeWithError(t);
				}

				@Override
				public CompletionStage<Void> getResult() {
					return ((HasEndOfStream) consumer).getEndOfStream();
				}
			};
		}
		StreamConsumerDecorator<T, Void> decorator = new StreamConsumerDecorator<T, Void>() {};
		decorator.setActualConsumer(consumer, decorator.getEndOfStream());
		return decorator;
	}

	public static <T, X> StreamConsumerWithResult<T, X> withResult(StreamConsumer<T> consumer, CompletionStage<X> result) {
		StreamConsumerDecorator<T, X> decorator = new StreamConsumerDecorator<T, X>() {};
		decorator.setActualConsumer(consumer, result);
		return decorator;
	}

	/**
	 * Returns {@link StreamConsumerToList} which saves received items in empty list
	 *
	 * @param eventloop event loop in which will run it
	 * @param <T>       type of item
	 */
	public static <T> StreamConsumerWithResult<T, List<T>> toList(Eventloop eventloop) {
		return new StreamConsumerToList<>(eventloop);
	}

	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> completionStage = SettableStage.create();
		private final Throwable exception;

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setProducer(StreamProducer<T> producer) {
			getCurrentEventloop().post(() -> {
				producer.closeWithError(exception);
				completionStage.trySetException(exception);
			});
		}

		@Override
		public void endOfStream() {
		}

		@Override
		public void closeWithError(Throwable t) {
			completionStage.trySetException(t);
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	static final class IdleImpl<T> implements StreamConsumer<T> {
		private StreamProducer<T> producer;

		@Override
		public void setProducer(StreamProducer<T> producer) {
			checkNotNull(producer);
			if (this.producer == producer) return;

			checkState(this.producer == null);

			this.producer = producer;
		}

		@Override
		public void endOfStream() {
		}

		@Override
		public void closeWithError(Throwable t) {
			producer.closeWithError(t);
		}

		public StreamProducer<T> getProducer() {
			return producer;
		}
	}
}
