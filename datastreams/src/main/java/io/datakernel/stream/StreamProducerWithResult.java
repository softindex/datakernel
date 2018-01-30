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
import io.datakernel.stream.StreamingResult.Pair;
import io.datakernel.stream.processor.StreamLateBinder;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.stream.DataStreams.bind;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;

public interface StreamProducerWithResult<T, X> extends StreamProducer<T> {
	CompletionStage<X> getResult();

	@SuppressWarnings("unchecked")
	@Override
	default StreamingProducerResult<X> streamTo(StreamConsumer<T> consumer) {
		StreamProducerWithResult<T, X> producer = this;
		bind(producer, consumer);
		CompletionStage<Void> producerEndOfStream = producer.getEndOfStream();
		CompletionStage<Void> consumerEndOfStream = consumer.getEndOfStream();
		CompletionStage<Void> endOfStream = Stages.run(producerEndOfStream, consumerEndOfStream);
		CompletionStage<X> producerResult = producer.getResult();
		return new StreamingProducerResult<X>() {
			@Override
			public CompletionStage<X> getProducerResult() {
				return producerResult;
			}

			@Override
			public CompletionStage<Void> getProducerEndOfStream() {
				return producerEndOfStream;
			}

			@Override
			public CompletionStage<Void> getConsumerEndOfStream() {
				return consumerEndOfStream;
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	default <Y> StreamingResult<X, Y> streamTo(StreamConsumerWithResult<T, Y> consumer) {
		StreamProducerWithResult<T, X> producer = this;
		bind(producer, consumer);
		CompletionStage<Void> producerEndOfStream = producer.getEndOfStream();
		CompletionStage<Void> consumerEndOfStream = consumer.getEndOfStream();
		CompletionStage<Void> endOfStream = Stages.run(producerEndOfStream, consumerEndOfStream);
		CompletionStage<Pair<X, Y>> result = Stages.pair(producer.getResult(), consumer.getResult())
				.thenApply(xyPair -> new StreamingResult.Pair<>(xyPair.getLeft(), xyPair.getRight()));
		return new StreamingResult<X, Y>() {
			@Override
			public CompletionStage<Pair<X, Y>> getResult() {
				return result;
			}

			@Override
			public CompletionStage<Void> getProducerEndOfStream() {
				return producerEndOfStream;
			}

			@Override
			public CompletionStage<Void> getConsumerEndOfStream() {
				return consumerEndOfStream;
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	@Override
	default <R> StreamProducerWithResult<R, X> with(StreamProducerModifier<T, R> modifier) {
		return modifier.apply(this).withResult(this.getResult());
	}

	@Override
	default StreamProducerWithResult<T, X> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : with(StreamLateBinder.create());
	}

	static <T, X> StreamProducerWithResult<T, X> ofStage(CompletionStage<StreamProducerWithResult<T, X>> producerStage) {
		SettableStage<X> result = SettableStage.create();
		StreamLateBinder<T> binder = StreamLateBinder.create();
		producerStage.whenComplete((producer, throwable) -> {
			if (throwable == null) {
				checkArgument(producer.getCapabilities().contains(LATE_BINDING),
						LATE_BINDING_ERROR_MESSAGE, producer);
				bind(producer, binder.getInput());
				producer.getResult().whenComplete(result::set);
			} else {
				bind(StreamProducer.closingWithError(throwable), binder.getInput());
				result.setException(throwable);
			}
		});
		return binder.getOutput().withResult(result);
	}

	default StreamProducerWithResult<T, X> whenComplete(BiConsumer<? super X, ? super Throwable> consumer) {
		getResult().whenComplete(consumer);
		return this;
	}

	default StreamProducerWithResult<T, X> thenAccept(Consumer<? super X> action) {
		getResult().thenAccept(action);
		return this;
	}

	default <U> StreamProducerWithResult<T, U> thenApply(Function<? super X, ? extends U> fn) {
		CompletionStage<X> stage = this.getResult();
		return withResult(stage.thenApply(fn));
	}

	default <U> StreamProducerWithResult<T, U> thenCompose(Function<? super X, ? extends CompletionStage<U>> fn) {
		SettableStage<U> resultStage = SettableStage.create();
		this.getResult().whenComplete((x, throwable) -> {
			if (throwable == null) {
				fn.apply(x).whenComplete(resultStage::set);
			} else {
				resultStage.setException(throwable);
			}
		});
		return withResult(resultStage);
	}

}