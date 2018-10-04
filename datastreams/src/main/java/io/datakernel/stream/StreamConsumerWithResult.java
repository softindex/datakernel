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

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;

import java.util.function.Function;

public final class StreamConsumerWithResult<T, X> {
	private final StreamConsumer<T> consumer;
	private final MaterializedStage<X> result;

	private StreamConsumerWithResult(StreamConsumer<T> consumer, MaterializedStage<X> result) {
		this.consumer = consumer;
		this.result = result;
	}

	public static <T, X> StreamConsumerWithResult<T, X> of(StreamConsumer<T> consumer, Stage<X> result) {
		return new StreamConsumerWithResult<>(consumer, result.materialize());
	}

	public StreamConsumerWithResult<T, X> sanitize() {
		return new StreamConsumerWithResult<>(consumer,
				consumer.getAcknowledgement().combine(result.whenException(consumer::close), ($, v) -> v).post());
	}

	public <T1, X1> StreamConsumerWithResult<T1, X1> transform(
			Function<StreamConsumer<T>, StreamConsumer<T1>> consumerTransformer,
			Function<Stage<X>, Stage<X1>> resultTransformer) {
		return new StreamConsumerWithResult<>(
				consumerTransformer.apply(consumer),
				resultTransformer.apply(result).materialize());
	}

	public <T1> StreamConsumerWithResult<T1, X> transformConsumer(Function<StreamConsumer<T>, StreamConsumer<T1>> consumerTransformer) {
		return transform(consumerTransformer, Function.identity());
	}

	public <X1> StreamConsumerWithResult<T, X1> transformResult(Function<Stage<X>, Stage<X1>> resultTransformer) {
		return transform(Function.identity(), resultTransformer);
	}

	public static <T, X> StreamConsumerWithResult<T, X> ofStage(Stage<StreamConsumerWithResult<T, X>> stage) {
		return of(
				StreamConsumer.ofStage(stage.thenApply(StreamConsumerWithResult::getConsumer)),
				stage.thenCompose(StreamConsumerWithResult::getResult));
	}

	public StreamConsumer<T> getConsumer() {
		return consumer;
	}

	public MaterializedStage<X> getResult() {
		return result;
	}
}
