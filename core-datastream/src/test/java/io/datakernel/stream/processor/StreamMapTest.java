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

import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.StreamMap.MapperProjection;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.stream.TestStreamConsumers.*;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(DatakernelRunner.class)
public class StreamMapTest {

	private static final MapperProjection<Integer, Integer> FUNCTION = new MapperProjection<Integer, Integer>() {
		@Override
		protected Integer apply(Integer input) {
			return input + 10;
		}
	};

	@Test
	public void test1() {

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3);
		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(source.transformWith(projection)
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(11, 12, 13), consumer.getList());
		assertEndOfStream(source);
		assertEndOfStream(projection.getInput());
		assertEndOfStream(projection.getOutput());
	}

	@Test
	public void testWithError() {
		List<Integer> list = new ArrayList<>();

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3);
		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);
		Exception exception = new Exception("Test Exception");

		Throwable e = awaitException(source.transformWith(projection)
				.streamTo(consumer
						.transformWith(decorator((context, dataAcceptor) ->
								item -> {
									dataAcceptor.accept(item);
									if (item == 12) {
										context.closeWithError(exception);
									}
								}))));

		assertSame(exception, e);
		assertEquals(2, list.size());
		assertClosedWithError(source);
		assertClosedWithError(consumer);
		assertClosedWithError(projection.getInput());
		assertClosedWithError(projection.getOutput());
	}

	@Test
	public void testSupplierWithError() {
		Exception exception = new Exception("Test Exception");
		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(1),
				StreamSupplier.of(2),
				StreamSupplier.closingWithError(exception));

		StreamMap<Integer, Integer> projection = StreamMap.create(FUNCTION);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		Throwable e = awaitException(source.transformWith(projection)
				.streamTo(consumer.transformWith(oneByOne())));

		assertSame(exception, e);
		assertEquals(2, list.size());
		assertClosedWithError(consumer);
	}

}
