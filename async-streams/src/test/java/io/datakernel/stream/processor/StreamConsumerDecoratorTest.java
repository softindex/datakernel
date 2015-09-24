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
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamConsumerDecoratorTest {
	@Test
	public void test2() {
		NioEventloop eventloop = new NioEventloop();

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);
		StreamConsumerDecorator<Integer> consumerDecorator = new StreamConsumerDecorator<>(consumer);

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()),
				StreamProducers.ofValue(eventloop, 1),
				StreamProducers.ofValue(eventloop, 2));

		producer.streamTo(consumerDecorator);
		eventloop.run();

		assertEquals(list, asList(1, 2, 3));
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
	}

	@Test
	public void test1() {
		NioEventloop eventloop = new NioEventloop();

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop);
		StreamConsumerDecorator<Integer> decorator = new StreamConsumerDecorator<>(consumer);
		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));

		producer.streamTo(decorator);

		eventloop.run();

		assertEquals(consumer.getList(), asList(1, 2, 3, 4, 5));
	}

}
