/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.datastream;

import io.datakernel.common.tuple.Tuple2;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public final class StreamSupplierWithResult<T, X> {
	@NotNull
	private final StreamSupplier<T> supplier;
	@NotNull
	private final Promise<X> result;

	private StreamSupplierWithResult(@NotNull StreamSupplier<T> supplier, @NotNull Promise<X> result) {
		this.supplier = supplier;
		this.result = result;
	}

	public static <T, X> StreamSupplierWithResult<T, X> of(StreamSupplier<T> supplier, Promise<X> result) {
		return new StreamSupplierWithResult<>(supplier, result);
	}

	public Promise<X> streamTo(StreamConsumer<T> consumer) {
		return supplier.streamTo(consumer)
				.then($ -> result);
	}

	public <Y> Promise<Tuple2<X, Y>> streamTo(StreamConsumerWithResult<T, Y> consumer) {
		return supplier.streamTo(consumer.getConsumer())
				.then($ -> Promises.toTuple(result, consumer.getResult()));
	}

	protected StreamSupplierWithResult<T, X> sanitize() {
		return new StreamSupplierWithResult<>(supplier,
				supplier.getEndOfStream().combine(result.whenException(supplier::close), ($, v) -> v).post());
	}

	public <T1, X1> StreamSupplierWithResult<T1, X1> transform(
			Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer,
			Function<Promise<X>, Promise<X1>> resultTransformer) {
		return new StreamSupplierWithResult<>(
				consumerTransformer.apply(supplier),
				resultTransformer.apply(result));
	}

	public <T1> StreamSupplierWithResult<T1, X> transformSupplier(Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer) {
		return transform(consumerTransformer, Function.identity());
	}

	public <X1> StreamSupplierWithResult<T, X1> transformResult(Function<Promise<X>, Promise<X1>> resultTransformer) {
		return transform(Function.identity(), resultTransformer);
	}

	public static <T, X> StreamSupplierWithResult<T, X> ofPromise(Promise<StreamSupplierWithResult<T, X>> promise) {
		if (promise.isResult()) return promise.getResult();
		return of(
				StreamSupplier.ofPromise(promise.map(StreamSupplierWithResult::getSupplier)),
				promise.then(StreamSupplierWithResult::getResult));
	}

	@NotNull
	public StreamSupplier<T> getSupplier() {
		return supplier;
	}

	@NotNull
	public Promise<X> getResult() {
		return result;
	}
}
