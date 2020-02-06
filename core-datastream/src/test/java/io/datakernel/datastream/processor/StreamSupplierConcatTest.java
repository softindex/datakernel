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
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.datastream.TestStreamConsumers.oneByOne;
import static io.datakernel.datastream.TestStreamConsumers.randomlySuspending;
import static io.datakernel.datastream.TestUtils.assertClosedWithError;
import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSupplierConcatTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testSequence() {
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(StreamSupplier.concat(
				StreamSupplier.of(1, 2, 3),
				StreamSupplier.of(4, 5, 6))
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
		assertEndOfStream(consumer);
	}

	@Test
	public void testSequenceException() {
		List<Integer> list = new ArrayList<>();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Throwable e = awaitException(StreamSupplier.concat(
				StreamSupplier.of(1, 2, 3),
				StreamSupplier.of(4, 5, 6),
				StreamSupplier.closingWithError(exception),
				StreamSupplier.of(1, 2, 3))
				.streamTo(consumer.transformWith(randomlySuspending())));

		assertSame(exception, e);
		assertEquals(asList(1, 2, 3, 4, 5, 6), list);
		assertClosedWithError(consumer);
	}

	@Test
	public void testConcat() {
		List<Integer> list = await(StreamSupplier.concat(
				StreamSupplier.of(1, 2, 3),
				StreamSupplier.of(4, 5, 6),
				StreamSupplier.of())
				.toList());

		assertEquals(asList(1, 2, 3, 4, 5, 6), list);
	}

	@Test
	public void testConcatException() {
		List<Integer> list = new ArrayList<>();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Throwable e = awaitException(StreamSupplier.concat(
				StreamSupplier.of(1, 2, 3),
				StreamSupplier.of(4, 5, 6),
				StreamSupplier.closingWithError(exception))
				.streamTo(consumer.transformWith(oneByOne())));

		assertSame(exception, e);
		assertEquals(asList(1, 2, 3, 4, 5, 6), list);

	}

}
