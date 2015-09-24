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

public class StreamProducerDecoratorTest {
	@SuppressWarnings("unchecked")
	@Test
	public void test2() {
		NioEventloop eventloop = new NioEventloop();
		List<Integer> list = new ArrayList<>();

		TestStreamConsumers.TestConsumerToList consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				if (item == 3) {
					upstreamProducer.onConsumerError(new Exception());
					return;
				}
				list.add(item);
				upstreamProducer.onConsumerSuspended();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.onConsumerResumed();
					}
				});
			}
		};

		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
		StreamProducerDecorator<Integer> producerDecorator = new StreamProducerDecorator<Integer>(eventloop, producer) {
		};

		producerDecorator.streamTo(consumer);

		eventloop.run();

		assertEquals(list, asList(1, 2));
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
//		assertTrue(producerDecorator.getStatus() == AbstractStreamProducer.CLOSED_WITH_ERROR);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test1() {
		NioEventloop eventloop = new NioEventloop();

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList consumer = TestStreamConsumers.toListOneByOne(eventloop, list);
		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
		StreamProducerDecorator<Integer> producerDecorator = new StreamProducerDecorator<Integer>(eventloop, producer) {
		};

		producerDecorator.streamTo(consumer);

		eventloop.run();

		assertEquals(consumer.getList(), asList(1, 2, 3, 4, 5));
		assertTrue(((AbstractStreamProducer)producer).getStatus() == AbstractStreamProducer.END_OF_STREAM);
//		assertTrue(producerDecorator.getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}
}
