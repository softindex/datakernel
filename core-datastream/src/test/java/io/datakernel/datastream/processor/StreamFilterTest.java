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

package io.datakernel.datastream.processor;

import io.datakernel.common.exception.ExpectedException;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;

import static io.datakernel.datastream.TestStreamTransformers.decorate;
import static io.datakernel.datastream.TestStreamTransformers.randomlySuspending;
import static io.datakernel.datastream.TestUtils.assertClosedWithError;
import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamFilterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamFilter<Integer> filter = StreamFilter.create(input -> input % 2 == 1);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(supplier.transformWith(filter)
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(1, 3, 5), consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(filter);
		assertEndOfStream(consumer);
	}

	@Test
	public void testWithError() {
		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamFilter<Integer> streamFilter = StreamFilter.create(input -> input % 2 != 1);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Throwable e = awaitException(source.transformWith(streamFilter)
				.streamTo(consumer
						.transformWith(decorate(promise ->
								promise.then(item -> item == 4 ? Promise.ofException(exception) : Promise.of(item))))));

		assertSame(exception, e);

		assertEquals(asList(2, 4), consumer.getList());
		assertClosedWithError(source);
		assertClosedWithError(consumer);
		assertClosedWithError(streamFilter);
	}

	@Test
	public void testSupplierDisconnectWithError() {
		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.ofIterable(Arrays.asList(1, 2, 3, 4, 5, 6)),
				StreamSupplier.closingWithError(exception));

		StreamFilter<Integer> streamFilter = StreamFilter.create(input -> input % 2 != 1);

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		Throwable e = awaitException(source.transformWith(streamFilter)
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(exception, e);

//		assertEquals(3, consumer.getList().size());
		assertClosedWithError(consumer);
		assertClosedWithError(streamFilter);
	}

	@Test
	public void testFilterConsumer() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6);
		StreamFilter<Integer> filter = StreamFilter.create(input -> input % 2 == 1);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		StreamConsumer<Integer> transformedConsumer = consumer
				.transformWith(filter)
				.transformWith(randomlySuspending());

		await(supplier.streamTo(transformedConsumer));

		assertEquals(asList(1, 3, 5), consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(filter);
	}

}
