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

package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialConsumerFunction;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSupplierFunction;

public interface SerialBidiFunction<I, O> extends
		SerialSupplierFunction<I, SerialSupplier<O>>,
		SerialConsumerFunction<O, SerialConsumer<I>> {

	static <T> SerialBidiFunction<T, T> identity() {
		return new SerialBidiFunction<T, T>() {
			@Override
			public SerialConsumer<T> apply(SerialConsumer<T> consumer) {
				return consumer;
			}

			@Override
			public SerialSupplier<T> apply(SerialSupplier<T> supplier) {
				return supplier;
			}
		};
	}
}
