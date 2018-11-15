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
public interface SerialOutput<T> {
	void set(SerialConsumer<T> output);

	default SerialSupplier<T> getSupplier() {
		return getSupplier(new SerialZeroBuffer<>());
	}

	default SerialSupplier<T> getSupplier(SerialQueue<T> queue) {
		set(queue.getConsumer());
		return queue.getSupplier();
	}

	default <R> SerialOutput<R> apply(SerialConsumerFunction<R, SerialConsumer<T>> fn) {
		return output -> set(output.apply(fn));
	}

	default <R> SerialOutput<R> transform(Function<? super T, ? extends R> fn) {
		return output -> set(output.transform(fn));
	}

	default <R> SerialOutput<R> transformAsync(Function<? super T, ? extends Promise<R>> fn) {
		return output -> set(output.transformAsync(fn));
	}

	default SerialOutput<T> filter(Predicate<? super T> predicate) {
		return output -> set(output.filter(predicate));
	}

	default SerialOutput<T> peek(Consumer<? super T> peek) {
		return output -> set(output.peek(peek));
	}

	default MaterializedPromise<Void> bindTo(SerialInput<T> to) {
		return bindTo(to, new SerialZeroBuffer<>());
	}

	default MaterializedPromise<Void> bindTo(SerialInput<T> to, SerialQueue<T> queue) {
		MaterializedPromise<Void> extraAcknowledgement = to.set(queue.getSupplier());
		set(queue.getConsumer(extraAcknowledgement));
		return extraAcknowledgement;
	}

}
