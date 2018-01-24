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

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public interface StreamConsumerWithResult<T, X> extends StreamConsumer<T> {
	CompletionStage<X> getResult();

	@Override
	default StreamConsumerWithResult<T, X> with(UnaryOperator<StreamConsumer<T>> modifier) {
		return modifier.apply(this).withResult(this.getResult());
	}

	static <T, X> StreamConsumerWithResult<T, X> ofStage(CompletionStage<StreamConsumerWithResult<T, X>> stage) {
		SettableStage<X> result = SettableStage.create();
		return new StreamConsumerDecorator<T>() {
			{
				stage.whenComplete((consumer1, throwable) -> {
					if (throwable == null) {
						setActualConsumer(consumer1);
						consumer1.getResult().whenComplete(result::set);
					} else {
						setActualConsumer(StreamConsumer.closingWithError(throwable));
						result.setException(throwable);
					}
				});
			}
		}.withResult(result);
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

	/**
	 * Returns {@link StreamConsumerToList} which saves received items in empty list
	 *
	 * @param <T> type of item
	 */
	static <T> StreamConsumerWithResult<T, List<T>> toList() {
		return new StreamConsumerToList<>();
	}
}
