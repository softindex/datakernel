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

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.stream.processor.StreamLateBinder;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;

public interface StreamConsumerWithResult<T, X> extends StreamConsumer<T> {
	Stage<X> getResult();

	@Override
	default <R> StreamConsumerWithResult<R, X> with(StreamConsumerModifier<T, R> modifier) {
		return modifier.applyTo(this).withResult(getResult());
	}

	@Override
	default StreamConsumerWithResult<T, X> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : with(StreamLateBinder.create());
	}

	static <T, X> StreamConsumerWithResult<T, X> ofStage(Stage<? extends StreamConsumerWithResult<T, X>> consumerStage) {
		SettableStage<X> result = new SettableStage<>();
		StreamLateBinder<T> binder = StreamLateBinder.create();
		consumerStage.whenComplete((consumer, throwable) -> {
			if (throwable == null) {
				assert consumer != null;
				checkArgument(consumer.getCapabilities().contains(LATE_BINDING),
						LATE_BINDING_ERROR_MESSAGE, consumer);
				binder.getOutput().streamTo(consumer);
				consumer.getResult().whenComplete(result::set);
			} else {
				binder.getOutput().streamTo(StreamConsumer.closingWithError(throwable));
				result.setException(throwable);
			}
		});
		return binder.getInput().withResult(result);
	}

	static <T> StreamConsumerWithResult<T, Void> ofAsyncConsumer(AsyncConsumer<T> consumer) {
		return new StreamConsumers.OfAsyncConsumerImpl<>(consumer);
	}

	default <U> StreamConsumerWithResult<T, U> thenApply(Function<? super X, ? extends U> fn) {
		return withResult(getResult().post().thenApply(fn));
	}

	default <U> StreamConsumerWithResult<T, U> thenApplyEx(BiFunction<? super X, Throwable, ? extends U> fn) {
		return withResult(getResult().post().thenApplyEx(fn));
	}

	default StreamConsumerWithResult<T, X> thenRun(Runnable action) {
		getResult().post().thenRun(action);
		return this;
	}

	default StreamConsumerWithResult<T, X> thenRunEx(Runnable action) {
		getResult().post().thenRunEx(action);
		return this;
	}

	default <U> StreamConsumerWithResult<T, U> thenCompose(Function<? super X, ? extends Stage<U>> fn) {
		return withResult(getResult().post().thenCompose(fn));
	}

	default <U> StreamConsumerWithResult<T, U> thenComposeEx(BiFunction<? super X, Throwable, ? extends Stage<U>> fn) {
		return withResult(getResult().post().thenComposeEx(fn));
	}

	default StreamConsumerWithResult<T, X> whenComplete(BiConsumer<? super X, Throwable> consumer) {
		getResult().post().whenComplete(consumer);
		return this;
	}

	default StreamConsumerWithResult<T, X> whenResult(Consumer<? super X> action) {
		getResult().post().whenResult(action);
		return this;
	}

	@Override
	default StreamConsumerWithResult<T, X> whenException(Consumer<Throwable> consumer) {
		getResult().post().whenException(consumer);
		return this;
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
