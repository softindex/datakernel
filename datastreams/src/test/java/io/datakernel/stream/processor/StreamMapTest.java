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
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.TestStreamConsumers;
import io.datakernel.stream.processor.StreamMap.MapperProjection;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamMapTest {

	private static final MapperProjection<Integer, Integer> FUNCTION = new MapperProjection<Integer, Integer>() {
		@Override
		protected Integer apply(Integer input) {
			return input + 10;
		}
	};

	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3);
		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		source.apply(projection).streamTo(
				consumer.apply(TestStreamConsumers.randomlySuspending()));

		eventloop.run();
		assertEquals(asList(11, 12, 13), consumer.getList());
		assertEndOfStream(source);
		assertEndOfStream(projection.getInput());
		assertEndOfStream(projection.getOutput());
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		List<Integer> list = new ArrayList<>();

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3);
		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		source.apply(projection).streamTo(
				consumer.apply(TestStreamConsumers.decorator((context, dataAcceptor) ->
						item -> {
							dataAcceptor.accept(item);
							if (item == 12) {
								context.closeWithError(new Exception("Test Exception"));
							}
						})));

		eventloop.run();
		assertEquals(2, list.size());
		assertClosedWithError(source);
		assertClosedWithError(consumer);
		assertClosedWithError(projection.getInput());
		assertClosedWithError(projection.getOutput());
	}

	@Test
	public void testSupplierWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(1),
				StreamSupplier.of(2),
				StreamSupplier.closingWithError(new Exception("Test Exception")));

		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		source.apply(projection).streamTo(
				consumer.apply(TestStreamConsumers.oneByOne()));

		eventloop.run();
		assertEquals(2, list.size());
		assertClosedWithError(consumer);
	}

}
