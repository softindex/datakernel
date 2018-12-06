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

package io.datakernel.examples;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of creating the custom StreamProducer.
 * This producer just streams all numbers from 0 to number specified in constructor.
 */
public class SupplierExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		StreamSupplier<Integer> supplier = StreamSupplier.of(0, 1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.streamTo(consumer);

		consumer.getResult().whenResult(result -> System.out.println("Consumer received: " + result));

		eventloop.run();
	}
}
