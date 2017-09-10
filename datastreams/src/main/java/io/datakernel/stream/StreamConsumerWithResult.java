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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.async.AsyncCallbacks.throwableToException;
import static io.datakernel.async.SettableStage.immediateFailedStage;
import static io.datakernel.async.SettableStage.immediateStage;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public final class StreamConsumerWithResult<T, X> implements StreamConsumer<T> {
	private StreamProducer<T> producer;
	private final StreamConsumer<T> wrappedConsumer;

	private final SettableStage<Void> completionStage = SettableStage.create();
	private final CompletionStage<X> resultStage;

	// region creators
	@SuppressWarnings("unchecked")
	private StreamConsumerWithResult(StreamConsumer<T> wrappedConsumer, CompletionStage<X> wrappedStage) {
		this.wrappedConsumer = wrappedConsumer;
		this.wrappedConsumer.streamFrom(new StreamProducer<T>() {
			@Override
			public void streamTo(StreamConsumer<T> consumer) {
				assert consumer == StreamConsumerWithResult.this.wrappedConsumer;
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				StreamConsumerWithResult.this.producer.produce(dataReceiver);
			}

			@Override
			public void suspend() {
				StreamConsumerWithResult.this.producer.suspend();
			}

			@Override
			public void closeWithError(Exception e) {
				StreamConsumerWithResult.this.producer.closeWithError(e);
			}
		});
		this.resultStage = Stages.all(completionStage, wrappedStage).thenApply(objects -> (X) objects[1])
				.whenComplete((x, throwable) -> {
					if (throwable != null) {
						producer.closeWithError(throwableToException(throwable));
						wrappedConsumer.closeWithError(throwableToException(throwable));
					}
				});
	}

	public static <T, X> StreamConsumerWithResult<T, X> create(StreamConsumer<T> wrappedConsumer, CompletionStage<X> wrappedStage) {
		return new StreamConsumerWithResult<>(wrappedConsumer, wrappedStage);
	}

	public static <T, X> StreamConsumerWithResult<T, X> wrap(StreamConsumer<T> wrappedConsumer, X result) {
		return new StreamConsumerWithResult<>(wrappedConsumer, immediateStage(result));
	}

	public static <T> StreamConsumerWithResult<T, Void> wrap(StreamConsumer<T> wrappedConsumer) {
		return wrap(wrappedConsumer, null);
	}

	public static <T, X> StreamConsumerWithResult<T, X> closingWithError(Throwable exception) {
		return create(StreamConsumers.idle(), immediateFailedStage(exception));
	}

	public static <T, X> StreamConsumerWithResult<T, X> ofStage(CompletionStage<StreamConsumerWithResult<T, X>> stage) {
		SettableStage<StreamConsumer<T>> consumer = SettableStage.create();
		SettableStage<X> result = SettableStage.create();
		stage.whenComplete((actualConsumer, throwable) -> {
			if (throwable == null) {
				consumer.set(actualConsumer);
				result.setStage(actualConsumer.getResult());
			} else {
				consumer.set(StreamConsumers.idle());
				result.setException(throwable);
			}
		});
		return StreamConsumerWithResult.create(StreamConsumers.ofStage(consumer), result);
	}

	// region creators

	@Override
	public void streamFrom(StreamProducer<T> producer) {
		checkNotNull(producer);
		if (this.producer == producer) return;

		checkState(this.producer == null);

		this.producer = producer;
		producer.streamTo(this);
	}

	@Override
	public void endOfStream() {
		if (completionStage.isSet()) return;
		completionStage.set(null);
		wrappedConsumer.endOfStream();
	}

	@Override
	public void closeWithError(Exception e) {
		if (completionStage.isSet()) return;
		completionStage.setException(e);
		wrappedConsumer.closeWithError(e);
	}

	public CompletionStage<X> getResult() {
		return resultStage;
	}
}
