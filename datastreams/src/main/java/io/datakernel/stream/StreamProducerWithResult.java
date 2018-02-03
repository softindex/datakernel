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
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.stream.StreamResult.Pair;
import io.datakernel.stream.processor.StreamLateBinder;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.async.NextStage.combine;
import static io.datakernel.stream.DataStreams.bind;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;

public interface StreamProducerWithResult<T, X> extends StreamProducer<T> {
	Stage<X> getResult();

	@SuppressWarnings("unchecked")
	@Override
	default StreamProducerResult<X> streamTo(StreamConsumer<T> consumer) {
		StreamProducerWithResult<T, X> producer = this;
		bind(producer, consumer);
		Stage<Void> producerEndOfStream = producer.getEndOfStream();
		Stage<Void> consumerEndOfStream = consumer.getEndOfStream();
		Stage<Void> endOfStream = Stages.all(producerEndOfStream, consumerEndOfStream);
		Stage<X> producerResult = producer.getResult();
		return new StreamProducerResult<X>() {
			@Override
			public Stage<X> getProducerResult() {
				return producerResult;
			}

			@Override
			public Stage<Void> getProducerEndOfStream() {
				return producerEndOfStream;
			}

			@Override
			public Stage<Void> getConsumerEndOfStream() {
				return consumerEndOfStream;
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	default <Y> StreamResult<X, Y> streamTo(StreamConsumerWithResult<T, Y> consumer) {
		StreamProducerWithResult<T, X> producer = this;
		bind(producer, consumer);
		Stage<Void> producerEndOfStream = producer.getEndOfStream();
		Stage<Void> consumerEndOfStream = consumer.getEndOfStream();
		Stage<Void> endOfStream = Stages.all(producerEndOfStream, consumerEndOfStream);
		Stage<Pair<X, Y>> result = producer.getResult().then(combine(consumer.getResult(), StreamResult.Pair::new));
		return new StreamResult<X, Y>() {
			@Override
			public Stage<Pair<X, Y>> getResult() {
				return result;
			}

			@Override
			public Stage<Void> getProducerEndOfStream() {
				return producerEndOfStream;
			}

			@Override
			public Stage<Void> getConsumerEndOfStream() {
				return consumerEndOfStream;
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	@Override
	default <R> StreamProducerWithResult<R, X> with(StreamProducerModifier<T, R> modifier) {
		return modifier.applyTo(this).withResult(this.getResult());
	}

	@Override
	default StreamProducerWithResult<T, X> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : with(StreamLateBinder.create());
	}

	static <T, X> StreamProducerWithResult<T, X> ofStage(Stage<StreamProducerWithResult<T, X>> producerStage) {
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

	default <U> StreamProducerWithResult<T, U> handle(Stage.Handler<? super X, U> handler) {
		return withResult(getResult().handle(handler));
	}

	default <U> StreamProducerWithResult<T, U> thenApply(Function<? super X, ? extends U> fn) {
		return withResult(getResult().thenApply(fn));
	}

	default StreamProducerWithResult<T, X> thenAccept(Consumer<? super X> action) {
		getResult().thenAccept(action);
		return this;
	}

	default StreamProducerWithResult<T, X> thenRun(Runnable action) {
		getResult().thenRun(action);
		return this;
	}

	default <U> StreamProducerWithResult<T, U> thenCompose(Function<? super X, ? extends Stage<U>> fn) {
		return withResult(getResult().thenCompose(fn));
	}

	default StreamProducerWithResult<T, X> whenComplete(BiConsumer<? super X, ? super Throwable> consumer) {
		getResult().whenComplete(consumer);
		return this;
	}

}