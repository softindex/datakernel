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

package io.datakernel.serial;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@FunctionalInterface
public interface SerialInput<T> {
	MaterializedPromise<Void> set(SerialSupplier<T> input);

	default SerialConsumer<T> getConsumer() {
		return getConsumer(new SerialZeroBuffer<>());
	}

	default SerialConsumer<T> getConsumer(SerialQueue<T> queue) {
		MaterializedPromise<Void> extraAcknowledge = set(queue.getSupplier());
		return queue.getConsumer(extraAcknowledge);
	}

	default <R> SerialInput<R> apply(SerialSupplierFunction<R, SerialSupplier<T>> fn) {
		return input -> SerialInput.this.set(fn.apply(input));
	}

	default <R> SerialInput<R> transform(Function<? super R, ? extends T> fn) {
		return input -> SerialInput.this.set(input.transform(fn));
	}

	default <R> SerialInput<R> transformAsync(Function<? super R, ? extends Promise<T>> fn) {
		return input -> SerialInput.this.set(input.transformAsync(fn));
	}

	default SerialInput<T> filter(Predicate<? super T> predicate) {
		return input -> SerialInput.this.set(input.filter(predicate));
	}

	default SerialInput<T> peek(Consumer<? super T> peek) {
		return input -> SerialInput.this.set(input.peek(peek));
	}

}
