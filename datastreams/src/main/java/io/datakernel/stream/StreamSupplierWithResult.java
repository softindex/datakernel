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

package io.datakernel.stream;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;

import java.util.function.Function;

public final class StreamSupplierWithResult<T, X> {
	private final StreamSupplier<T> supplier;
	private final MaterializedPromise<X> result;

	private StreamSupplierWithResult(StreamSupplier<T> supplier, MaterializedPromise<X> result) {
		this.supplier = supplier;
		this.result = result;
	}

	public static <T, X> StreamSupplierWithResult<T, X> of(StreamSupplier<T> supplier, Promise<X> result) {
		return new StreamSupplierWithResult<>(supplier, result.materialize());
	}

	public StreamSupplierWithResult<T, X> sanitize() {
		return new StreamSupplierWithResult<>(supplier,
				supplier.getEndOfStream().combine(result.whenException(supplier::close), ($, v) -> v).post());
	}

	public <T1, X1> StreamSupplierWithResult<T1, X1> transform(
			Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer,
			Function<Promise<X>, Promise<X1>> resultTransformer) {
		return new StreamSupplierWithResult<>(
				consumerTransformer.apply(supplier),
				resultTransformer.apply(result).materialize());
	}

	public <T1> StreamSupplierWithResult<T1, X> transformSupplier(Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer) {
		return transform(consumerTransformer, Function.identity());
	}

	public <X1> StreamSupplierWithResult<T, X1> transformResult(Function<Promise<X>, Promise<X1>> resultTransformer) {
		return transform(Function.identity(), resultTransformer);
	}

	public static <T, X> StreamSupplierWithResult<T, X> ofPromise(Promise<StreamSupplierWithResult<T, X>> promise) {
		if (promise.isResult()) return promise.materialize().getResult();
		return of(
				StreamSupplier.ofPromise(promise.thenApply(StreamSupplierWithResult::getSupplier)),
				promise.thenCompose(StreamSupplierWithResult::getResult));
	}

	public StreamSupplier<T> getSupplier() {
		return supplier;
	}

	public MaterializedPromise<X> getResult() {
		return result;
	}
}
