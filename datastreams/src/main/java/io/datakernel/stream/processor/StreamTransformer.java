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

public interface StreamTransformer<I, O> extends
		StreamInput<I>, StreamOutput<O>,
		StreamSupplierFunction<I, StreamSupplier<O>>,
		StreamConsumerFunction<O, StreamConsumer<I>> {

	static <X> StreamTransformer<X, X> identity() {
		return StreamFunction.create(Function.identity());
	}

	@Override
	default StreamConsumer<I> apply(StreamConsumer<O> consumer) {
		getOutput().streamTo(consumer);
		return getInput();
	}

	@Override
	default StreamSupplier<O> apply(StreamSupplier<I> supplier) {
		supplier.streamTo(getInput());
		return getOutput();
	}

}
