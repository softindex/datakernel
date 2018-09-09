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

public final class StreamProducerWithResult<T, X> {
	private final StreamProducer<T> producer;
	private final MaterializedStage<X> result;

	private StreamProducerWithResult(StreamProducer<T> producer, MaterializedStage<X> result) {
		this.producer = producer;
		this.result = result;
	}

	public static <T, X> StreamProducerWithResult<T, X> of(StreamProducer<T> producer, Stage<X> result) {
		return new StreamProducerWithResult<>(producer, result.materialize());
	}

	public StreamProducerWithResult<T, X> sanitize() {
		return new StreamProducerWithResult<>(producer,
				producer.getEndOfStream().combine(result.whenException(producer::closeWithError), ($, v) -> v).post());
	}

	public <T1, X1> StreamProducerWithResult<T1, X1> transform(
			Function<StreamProducer<T>, StreamProducer<T1>> consumerTransformer,
			Function<Stage<X>, Stage<X1>> resultTransformer) {
		return new StreamProducerWithResult<>(
				consumerTransformer.apply(producer),
				resultTransformer.apply(result).materialize());
	}

	public <T1> StreamProducerWithResult<T1, X> transformProducer(Function<StreamProducer<T>, StreamProducer<T1>> consumerTransformer) {
		return transform(consumerTransformer, Function.identity());
	}

	public <X1> StreamProducerWithResult<T, X1> transformResult(Function<Stage<X>, Stage<X1>> resultTransformer) {
		return transform(Function.identity(), resultTransformer);
	}

	public static <T, X> StreamProducerWithResult<T, X> ofStage(Stage<StreamProducerWithResult<T, X>> stage) {
		if (stage.hasResult()) return stage.getResult();
		return of(
				StreamProducer.ofStage(stage.thenApply(StreamProducerWithResult::getProducer)),
				stage.thenCompose(StreamProducerWithResult::getResult));
	}

	public StreamProducer<T> getProducer() {
		return producer;
	}

	public MaterializedStage<X> getResult() {
		return result;
	}
}
