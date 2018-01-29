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
import io.datakernel.stream.processor.StreamLateBinder;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.datakernel.stream.DataStreams.bind;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;

public interface StreamConsumerWithResult<T, X> extends StreamConsumer<T> {
	CompletionStage<X> getResult();

	@Override
	default <R> StreamConsumerWithResult<R, X> with(StreamConsumerModifier<T, R> modifier) {
		return modifier.apply(this).withResult(this.getResult());
	}

	@Override
	default StreamConsumerWithResult<T, X> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : with(StreamLateBinder.create());
	}

	static <T, X> StreamConsumerWithResult<T, X> ofStage(CompletionStage<StreamConsumerWithResult<T, X>> consumerStage) {
		SettableStage<X> result = SettableStage.create();
		StreamLateBinder<T> binder = StreamLateBinder.create();
		consumerStage.whenComplete((consumer, throwable) -> {
			if (throwable == null) {
				checkArgument(consumer.getCapabilities().contains(LATE_BINDING),
						LATE_BINDING_ERROR_MESSAGE, consumer);
				bind(binder.getOutput(), consumer);
				consumer.getResult().whenComplete(result::set);
			} else {
				bind(binder.getOutput(), StreamConsumer.closingWithError(throwable));
				result.setException(throwable);
			}
		});
		return binder.getInput().withResult(result);
	}

	default StreamConsumerWithResult<T, X> whenComplete(BiConsumer<? super X, ? super Throwable> consumer) {
		getResult().whenComplete(consumer);
		return this;
	}

	default StreamConsumerWithResult<T, X> thenAccept(Consumer<? super X> action) {
		getResult().thenAccept(action);
		return this;
	}

	default <U> StreamConsumerWithResult<T, U> thenApply(Function<? super X, ? extends U> fn) {
		CompletionStage<X> stage = this.getResult();
		return withResult(stage.thenApply(fn));
	}

	default <U> StreamConsumerWithResult<T, U> thenCompose(Function<? super X, ? extends CompletionStage<U>> fn) {
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

	/**
	 * Returns {@link StreamConsumerToList} which saves received items in empty list
	 *
	 * @param <T> type of item
	 */
	static <T> StreamConsumerWithResult<T, List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	static <T, A, R> StreamConsumerWithResult<T, R> toCollector(Collector<T, A, R> collector) {
		return new StreamConsumerToCollector<>(collector);
	}
}
