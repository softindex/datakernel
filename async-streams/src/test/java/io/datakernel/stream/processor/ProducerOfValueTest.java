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

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProducerOfValueTest {
	NioEventloop eventloop = new NioEventloop();

	String TEST_STRING = "Hello consumer";
	Integer TEST_INT = 777;
	DataItem1 TEST_OBJECT = new DataItem1(1, 1, 8, 8);
	Object TEST_NULL = null;

	@Test
	public void test1() {
		StreamConsumers.ToList<Integer> consumer1 = new StreamConsumers.ToList<>(eventloop, new LinkedList<Integer>());
		StreamProducers.OfValue<Integer> producer1 = new StreamProducers.OfValue<>(eventloop, TEST_INT);
		producer1.streamTo(consumer1);

		eventloop.run();

		assertEquals(TEST_INT, consumer1.getList().get(0));
		assertTrue(producer1.getStatus() == AbstractStreamProducer.END_OF_STREAM);

		StreamConsumers.ToList<String> consumer2 = new StreamConsumers.ToList<>(eventloop, new LinkedList<String>());
		StreamProducers.OfValue<String> producer2 = new StreamProducers.OfValue<>(eventloop, TEST_STRING);
		producer2.streamTo(consumer2);
		eventloop.run();

		assertEquals(TEST_STRING, consumer2.getList().get(0));
		assertTrue(producer2.getStatus() == AbstractStreamProducer.END_OF_STREAM);

		StreamConsumers.ToList<DataItem1> consumer3 = new StreamConsumers.ToList<>(eventloop, new LinkedList<DataItem1>());
		StreamProducers.OfValue<DataItem1> producer3 = new StreamProducers.OfValue<>(eventloop, TEST_OBJECT);
		producer3.streamTo(consumer3);
		eventloop.run();

		assertEquals(TEST_OBJECT, consumer3.getList().get(0));
		assertTrue(producer3.getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void testNull() {
		StreamConsumers.ToList<Object> consumer3 = new StreamConsumers.ToList<>(eventloop, new LinkedList<>());
		StreamProducers.OfValue<Object> producer3 = new StreamProducers.OfValue<>(eventloop, TEST_NULL);
		producer3.streamTo(consumer3);
		eventloop.run();

		assertTrue(consumer3.getList().get(0) == null);
		assertTrue(producer3.getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

}
