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
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.LinkedList;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProducerOfValueTest {
	Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

	String TEST_STRING = "Hello consumer";
	Integer TEST_INT = 777;
	DataItem1 TEST_OBJECT = new DataItem1(1, 1, 8, 8);
	Object TEST_NULL = null;

	@Test
	public void test1() {
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create(new LinkedList<Integer>());
		StreamProducer<Integer> producer1 = StreamProducer.of(TEST_INT);
		producer1.streamTo(consumer1);

		eventloop.run();

		assertEquals(TEST_INT, consumer1.getList().get(0));
		assertEndOfStream(producer1);

		StreamConsumerToList<String> consumer2 = StreamConsumerToList.create(new LinkedList<String>());
		StreamProducer<String> producer2 = StreamProducer.of(TEST_STRING);
		producer2.streamTo(consumer2);
		eventloop.run();

		assertEquals(TEST_STRING, consumer2.getList().get(0));
		assertEndOfStream(producer2);

		StreamConsumerToList<DataItem1> consumer3 = StreamConsumerToList.create(new LinkedList<DataItem1>());
		StreamProducer<DataItem1> producer3 = StreamProducer.of(TEST_OBJECT);
		producer3.streamTo(consumer3);
		eventloop.run();

		assertEquals(TEST_OBJECT, consumer3.getList().get(0));
		assertEndOfStream(producer3);
	}

	@Test
	public void testNull() {
		StreamConsumerToList<Object> consumer3 = StreamConsumerToList.create(new LinkedList<>());
		StreamProducer<Object> producer3 = StreamProducer.of(TEST_NULL);
		producer3.streamTo(consumer3);
		eventloop.run();

		assertTrue(consumer3.getList().get(0) == null);
		assertEndOfStream(producer3);
	}

}
