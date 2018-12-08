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
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestStreamConsumers.*;
import static io.datakernel.stream.TestUtils.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamSharderTest {

	private static final Sharder<Integer> SHARDER = object -> object % 2;

	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		source.streamTo(streamSharder.getInput());

		streamSharder.newOutput().streamTo(consumer1.transformWith(randomlySuspending()));
		streamSharder.newOutput().streamTo(consumer2.transformWith(randomlySuspending()));

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertEndOfStream(source);
		assertEndOfStream(streamSharder.getInput());
		assertEndOfStream(streamSharder.getOutput(0));
		assertEndOfStream(consumer1);
		assertEndOfStream(consumer2);
	}

	@Test
	public void test2() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1.transformWith(randomlySuspending()));
		streamSharder.newOutput().streamTo(consumer2.transformWith(randomlySuspending()));

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertEndOfStream(source);
		assertEndOfStream(source);
		assertEndOfStream(streamSharder.getInput());
		assertEndOfStream(streamSharder.getOutput(0));
		assertEndOfStream(consumer1);
		assertEndOfStream(consumer2);
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create(list1);

		List<Integer> list2 = new ArrayList<>();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create(list2);

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(
				consumer2.transformWith(decorator((context, dataAcceptor) ->
						item -> {
							dataAcceptor.accept(item);
							if (item == 3) {
								context.closeWithError(new ExpectedException("Test Exception"));
							}
						})));

		eventloop.run();

		assertEquals(1, list1.size());
		assertEquals(2, list2.size());
		assertClosedWithError(source);
		assertClosedWithError(source);
		assertClosedWithError(streamSharder.getInput());
		assertSuppliersClosedWithError(streamSharder.getOutputs());
		assertClosedWithError(consumer1);
		assertClosedWithError(consumer2);
	}

	@Test
	public void testSupplierWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(1),
				StreamSupplier.of(2),
				StreamSupplier.of(3),
				StreamSupplier.closingWithError(new ExpectedException("Test Exception"))
		);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumer<Integer> consumer1 = StreamConsumerToList.create(list1);
		List<Integer> list2 = new ArrayList<>();
		StreamConsumer<Integer> consumer2 = StreamConsumerToList.create(list2);

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1.transformWith(oneByOne()));
		streamSharder.newOutput().streamTo(consumer2.transformWith(oneByOne()));

		eventloop.run();

		assertEquals(1, list1.size());
		assertEquals(2, list2.size());

		assertClosedWithError(streamSharder.getInput());
		assertSuppliersClosedWithError(streamSharder.getOutputs());
		assertClosedWithError(consumer1);
		assertClosedWithError(consumer2);
	}
}
