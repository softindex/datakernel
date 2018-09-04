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

package io.datakernel.stream.processor;

import io.datakernel.stream.*;

import java.util.function.Function;

public interface StreamTransformer<I, O> extends HasInput<I>, HasOutput<O>, StreamModifier<I, O> {

	static <X> StreamTransformer<X, X> idenity() {
		return StreamFunction.create(Function.identity());
	}

	default Function<StreamProducer<I>, StreamProducer<O>> transformer() {
		return input -> {
			input.streamTo(getInput());
			return getOutput();
		};
	}

	default <T> StreamTransformer<T, O> with(StreamConsumerModifier<I, T> consumerModifier) {
		return new StreamTransformer<T, O>() {
			private final StreamConsumer<T> input = consumerModifier.applyTo(StreamTransformer.this.getInput());

			@Override
			public StreamConsumer<T> getInput() {
				return input;
			}

			@Override
			public StreamProducer<O> getOutput() {
				return StreamTransformer.this.getOutput();
			}
		};
	}

	default <T> StreamTransformer<I, T> with(StreamProducerModifier<O, T> producerModifier) {
		return new StreamTransformer<I, T>() {
			private final StreamProducer<T> output = producerModifier.applyTo(StreamTransformer.this.getOutput());

			@Override
			public StreamConsumer<I> getInput() {
				return StreamTransformer.this.getInput();
			}

			@Override
			public StreamProducer<T> getOutput() {
				return output;
			}
		};
	}

	@Override
	default StreamConsumer<I> applyTo(StreamConsumer<O> consumer) {
		getOutput().streamTo(consumer);
		return getInput();
	}

	@Override
	default StreamProducer<O> applyTo(StreamProducer<I> producer) {
		producer.streamTo(getInput());
		return getOutput();
	}

	default Function<StreamProducer<I>, StreamProducer<O>> toProducer() {
		return this::applyTo;
	}

	default Function<StreamConsumer<O>, StreamConsumer<I>> toConsumer() {
		return this::applyTo;
	}

}
