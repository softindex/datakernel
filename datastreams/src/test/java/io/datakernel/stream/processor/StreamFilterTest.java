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
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestStreamConsumers.decorator;
import static io.datakernel.stream.TestStreamConsumers.randomlySuspending;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamFilterTest {
	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3);
		StreamFilter<Integer> filter = StreamFilter.create(input -> input % 2 == 1);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.apply(filter).streamTo(
				consumer.apply(randomlySuspending()));

		eventloop.run();
		assertEquals(asList(1, 3), consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(filter.getInput());
		assertEndOfStream(filter.getOutput());
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		List<Integer> list = new ArrayList<>();

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4, 5);
		StreamFilter<Integer> streamFilter = StreamFilter.create(input -> input % 2 != 2);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		source.apply(streamFilter).streamTo(
				consumer.apply(decorator((context, dataAcceptor) ->
						item -> {
							dataAcceptor.accept(item);
							if (item == 3) {
								context.closeWithError(new ExpectedException("Test Exception"));
							}
						})));

		eventloop.run();

		assertEquals(asList(1, 2, 3), list);
		assertClosedWithError(source);
		assertClosedWithError(consumer);
		assertClosedWithError(streamFilter.getInput());
		assertClosedWithError(streamFilter.getOutput());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSupplierDisconnectWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.ofIterable(Arrays.asList(1, 2, 3)),
				StreamSupplier.closingWithError(new ExpectedException("Test Exception")));

		StreamFilter<Integer> streamFilter = StreamFilter.create(input -> input % 2 != 2);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		source.apply(streamFilter).streamTo(
				consumer.apply(TestStreamConsumers.oneByOne()));

		eventloop.run();

		assertEquals(3, list.size());
		assertClosedWithError(consumer);
		assertClosedWithError(streamFilter.getInput());
		assertClosedWithError(streamFilter.getOutput());
	}

}
