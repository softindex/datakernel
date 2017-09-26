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
import io.datakernel.async.Stages.Tuple2;

import java.util.concurrent.CompletionStage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.async.SettableStage.immediateFailedStage;
import static io.datakernel.async.SettableStage.immediateStage;

public final class StreamProducerWithResultImpl<T, X> implements StreamProducerWithResult<T, X> {
	private final StreamProducer<T> wrappedProducer;
	private StreamConsumer<T> consumer;
	private final SettableStage<Void> completionStage = SettableStage.create();

	private final CompletionStage<X> resultStage;

	// region creators
	@SuppressWarnings("unchecked")
	private StreamProducerWithResultImpl(StreamProducer<T> wrappedProducer, CompletionStage<X> wrappedResult) {
		this.wrappedProducer = wrappedProducer;
		this.wrappedProducer.streamTo(new StreamConsumer<T>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				assert producer == StreamProducerWithResultImpl.this.wrappedProducer;
			}

			@Override
			public void endOfStream() {
				completionStage.trySet(null);
			}

			@Override
			public void closeWithError(Throwable t) {
				completionStage.trySetException(t);
				consumer.closeWithError(t);
			}
		});
		this.resultStage = Stages.tuple(completionStage, wrappedResult)
				.thenApply(Tuple2::getValue2)
				.whenCompleteAsync((x, throwable) -> {
					if (throwable == null) {
						consumer.endOfStream();
					} else {
						wrappedProducer.closeWithError(throwable);
						consumer.closeWithError(throwable);
					}
				});
	}

	public static <T, X> StreamProducerWithResult<T, X> create(StreamProducer<T> wrappedProducer, CompletionStage<X> wrappedResult) {
		return new StreamProducerWithResultImpl<>(wrappedProducer, wrappedResult);
	}

	public static <T, X> StreamProducerWithResult<T, X> wrap(StreamProducer<T> wrappedProducer, X result) {
		return new StreamProducerWithResultImpl<>(wrappedProducer, immediateStage(result));
	}

	public static <T> StreamProducerWithResult<T, Void> wrap(StreamProducer<T> wrappedProducer) {
		return wrap(wrappedProducer, null);
	}

	public static <T, X> StreamProducerWithResult<T, X> closingWithError(Throwable exception) {
		return create(StreamProducers.idle(), immediateFailedStage(exception));
	}

	public static <T, X> StreamProducerWithResult<T, X> ofStage(CompletionStage<StreamProducerWithResult<T, X>> stage) {
		SettableStage<StreamProducer<T>> producer = SettableStage.create();
		SettableStage<X> result = SettableStage.create();
		stage.whenComplete((actualProducer, throwable) -> {
			if (throwable == null) {
				producer.set(actualProducer);
				result.setStage(actualProducer.getResult());
			} else {
				producer.set(StreamProducers.idle());
				result.setException(throwable);
			}
		});
		return StreamProducerWithResultImpl.create(StreamProducers.ofStage(producer), result);
	}
	// endregion

	@Override
	public void setConsumer(StreamConsumer<T> consumer) {
		checkNotNull(consumer);
		checkState(this.consumer == null);
		this.consumer = consumer;
	}

	@Override
	public void produce(StreamDataReceiver<T> dataReceiver) {
		wrappedProducer.produce(dataReceiver);
	}

	@Override
	public void suspend() {
		wrappedProducer.suspend();
	}

	@Override
	public void closeWithError(Throwable t) {
		wrappedProducer.closeWithError(t);
	}

	public CompletionStage<X> getResult() {
		return resultStage;
	}

}