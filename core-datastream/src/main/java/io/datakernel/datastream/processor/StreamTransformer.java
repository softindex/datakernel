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

package io.datakernel.datastream.processor;

import io.datakernel.datastream.*;

import java.util.function.Function;

/**
 * A bidirectional stream transformer capable of transforming both
 * {@link StreamConsumer consumers} and {@link StreamSupplier suppliers}.
 *
 * @param <I> input elements type
 * @param <O> output elements type.
 */
public interface StreamTransformer<I, O> extends HasStreamInput<I>, HasStreamOutput<O>,
		StreamSupplierTransformer<I, StreamSupplier<O>>,
		StreamConsumerTransformer<O, StreamConsumer<I>> {

	/**
	 * An identity transformer that does not change the elements.
	 */
	static <X> StreamTransformer<X, X> identity() {
		return StreamMapper.create(Function.identity());
	}

	@Override
	default StreamConsumer<I> transform(StreamConsumer<O> consumer) {
		getOutput().streamTo(consumer);
		return getInput();
	}

	@Override
	default StreamSupplier<O> transform(StreamSupplier<I> supplier) {
		supplier.streamTo(getInput());
		return getOutput();
	}

}
