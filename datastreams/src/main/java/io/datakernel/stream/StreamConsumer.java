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

import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;

import static io.datakernel.async.SettableStage.mirrorOf;
import static io.datakernel.async.Stages.onError;

/**
 * It represents an object which can asynchronous receive streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of input data
 */
public interface StreamConsumer<T> {
	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param producer stream producer for setting
	 */
	void setProducer(StreamProducer<T> producer);

	CompletionStage<Void> getEndOfStream();

	static <T> StreamConsumer<T> idle() {
		return new StreamConsumers.IdleImpl<>();
	}

	static <T> StreamConsumer<T> closingWithError(Throwable exception) {
		return new StreamConsumers.ClosingWithErrorImpl<>(exception);
	}

	static <T> StreamConsumer<T> ofStage(CompletionStage<StreamConsumer<T>> consumerStage) {
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

	default <X> StreamConsumerWithResult<T, X> withResult(CompletionStage<X> result) {
		SettableStage<X> safeResult = mirrorOf(result);
		getEndOfStream().whenComplete(onError(safeResult::trySetException));
		return new StreamConsumerWithResult<T, X>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				StreamConsumer.this.setProducer(producer);
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return StreamConsumer.this.getEndOfStream();
			}

			@Override
			public CompletionStage<X> getResult() {
				return safeResult;
			}
		};
	}

	default StreamConsumerWithResult<T, Void> withEndOfStreamAsResult() {
		return new StreamConsumerWithResult<T, Void>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				StreamConsumer.this.setProducer(producer);
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return StreamConsumer.this.getEndOfStream();
			}

			@Override
			public CompletionStage<Void> getResult() {
				return StreamConsumer.this.getEndOfStream();
			}
		};
	}

	default StreamConsumer<T> with(UnaryOperator<StreamConsumer<T>> wrapper) {
		return wrapper.apply(this);
	}

}
