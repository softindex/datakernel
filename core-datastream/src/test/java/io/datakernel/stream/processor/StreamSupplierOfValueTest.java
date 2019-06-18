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
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.LinkedList;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamSupplierOfValueTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	private static final String TEST_STRING = "Hello consumer";
	private static final Integer TEST_INT = 777;
	private static final DataItem1 TEST_OBJECT = new DataItem1(1, 1, 8, 8);
	private static final Object TEST_NULL = null;

	@Test
	public void test1() {
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create(new LinkedList<>());
		StreamSupplier<Integer> supplier1 = StreamSupplier.of(TEST_INT);
		await(supplier1.streamTo(consumer1));

		assertEquals(TEST_INT, consumer1.getList().get(0));
		assertEndOfStream(supplier1);

		StreamConsumerToList<String> consumer2 = StreamConsumerToList.create(new LinkedList<>());
		StreamSupplier<String> supplier2 = StreamSupplier.of(TEST_STRING);
		await(supplier2.streamTo(consumer2));

		assertEquals(TEST_STRING, consumer2.getList().get(0));
		assertEndOfStream(supplier2);

		StreamConsumerToList<DataItem1> consumer3 = StreamConsumerToList.create(new LinkedList<>());
		StreamSupplier<DataItem1> supplier3 = StreamSupplier.of(TEST_OBJECT);
		await(supplier3.streamTo(consumer3));

		assertEquals(TEST_OBJECT, consumer3.getList().get(0));
		assertEndOfStream(supplier3);
	}

	@Test
	public void testNull() {
		StreamConsumerToList<Object> consumer3 = StreamConsumerToList.create(new LinkedList<>());
		StreamSupplier<Object> supplier3 = StreamSupplier.of(TEST_NULL);
		await(supplier3.streamTo(consumer3));

		assertNull(consumer3.getList().get(0));
		assertEndOfStream(supplier3);
	}

}
