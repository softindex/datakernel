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

public final class StreamSupplierWithResult<T, X> {
	private final StreamSupplier<T> supplier;
	private final MaterializedStage<X> result;

	private StreamSupplierWithResult(StreamSupplier<T> supplier, MaterializedStage<X> result) {
		this.supplier = supplier;
		this.result = result;
	}

	public static <T, X> StreamSupplierWithResult<T, X> of(StreamSupplier<T> supplier, Stage<X> result) {
		return new StreamSupplierWithResult<>(supplier, result.materialize());
	}

	public StreamSupplierWithResult<T, X> sanitize() {
		return new StreamSupplierWithResult<>(supplier,
				supplier.getEndOfStream().combine(result.whenException(supplier::close), ($, v) -> v).post());
	}

	public <T1, X1> StreamSupplierWithResult<T1, X1> transform(
			Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer,
			Function<Stage<X>, Stage<X1>> resultTransformer) {
		return new StreamSupplierWithResult<>(
				consumerTransformer.apply(supplier),
				resultTransformer.apply(result).materialize());
	}

	public <T1> StreamSupplierWithResult<T1, X> transformSupplier(Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer) {
		return transform(consumerTransformer, Function.identity());
	}

	public <X1> StreamSupplierWithResult<T, X1> transformResult(Function<Stage<X>, Stage<X1>> resultTransformer) {
		return transform(Function.identity(), resultTransformer);
	}

	public static <T, X> StreamSupplierWithResult<T, X> ofStage(Stage<StreamSupplierWithResult<T, X>> stage) {
		if (stage.hasResult()) return stage.getResult();
		return of(
				StreamSupplier.ofStage(stage.thenApply(StreamSupplierWithResult::getSupplier)),
				stage.thenCompose(StreamSupplierWithResult::getResult));
	}

	public StreamSupplier<T> getSupplier() {
		return supplier;
	}

	public MaterializedStage<X> getResult() {
		return result;
	}
}
