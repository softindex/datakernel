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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamSupplier.concat;
import static io.datakernel.stream.TestStreamConsumers.decorator;
import static io.datakernel.stream.TestStreamConsumers.randomlySuspending;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamFunctionTest {

	@Test
	public void testFunction() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.transformWith(StreamDecorator.create(input -> input * input))
				.streamTo(consumer.transformWith(randomlySuspending()));
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertEndOfStream(supplier);
//		assertEndOfStream(streamFunction.getInput());
//		assertEndOfStream(streamFunction.getOutput());
		assertEndOfStream(consumer);
	}

	@Test
	public void testFunctionConsumerError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamDecorator<Integer, Integer> streamFunction = StreamDecorator.create(input -> input * input);

		List<Integer> list = new ArrayList<>();
		StreamSupplier<Integer> source1 = StreamSupplier.of(1, 2, 3);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		source1.transformWith(streamFunction)
				.streamTo(consumer
						.transformWith(decorator((context, dataAcceptor) ->
								item -> {
									dataAcceptor.accept(item);
									if (list.size() == 2) {
										context.closeWithError(new ExpectedException("Test Exception"));
									}
								})));
		eventloop.run();

		assertEquals(asList(1, 4), list);

		assertClosedWithError(source1);
		assertClosedWithError(consumer);
		assertClosedWithError(streamFunction.getInput());
		assertClosedWithError(streamFunction.getOutput());
	}

	@Test
	public void testFunctionSupplierError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamDecorator<Integer, Integer> streamFunction = StreamDecorator.create(input -> input * input);

		StreamSupplier<Integer> supplier = concat(
				StreamSupplier.of(1, 2, 3),
				StreamSupplier.of(4, 5, 6),
				StreamSupplier.closingWithError(new ExpectedException("Test Exception")));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.transformWith(streamFunction)
				.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9, 16, 25, 36), consumer.getList());

		assertClosedWithError(consumer.getSupplier());
		assertClosedWithError(streamFunction.getInput());
		assertClosedWithError(streamFunction.getOutput());
	}

	@Test
	public void testIdentity() {
		Function<String, String> function1 = Function.identity();
		Function<Integer, Integer> function2 = Function.identity();
		assertSame(function1, function2);
	}

}
