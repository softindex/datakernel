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

import io.datakernel.eventloop.EventloopStub;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConsumerToListTest {

	@Test
	public void emptyListTest() {
		EventloopStub eventloopStub = new EventloopStub();
		StreamConsumers.ToList<String> consumer = new StreamConsumers.ToList<>(eventloopStub, new ArrayList<String>());

		List<String> testList2 = new ArrayList<>();
		testList2.add("a");
		testList2.add("b");
		testList2.add("c");
		testList2.add("d");

		StreamProducer<String> producer = StreamProducers.ofIterable(eventloopStub, testList2);
		producer.streamTo(consumer);
		eventloopStub.run();

		assertEquals(testList2, consumer.getList());
		assertTrue(producer.getStatus() == StreamProducer.CLOSED);
	}

	@Test
	public void fullListTest() {
		EventloopStub eventloopStub = new EventloopStub();
		List<Integer> testList1 = new ArrayList<>();
		testList1.add(1);
		testList1.add(2);
		testList1.add(3);
		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloopStub, testList1);

		List<Integer> testList2 = new ArrayList<>();
		testList2.add(4);
		testList2.add(5);
		testList2.add(6);

		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloopStub, testList2);
		producer.streamTo(consumer);
		eventloopStub.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
		assertTrue(producer.getStatus() == StreamProducer.CLOSED);
	}

}
