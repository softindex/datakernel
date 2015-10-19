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

import com.google.common.base.Functions;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerDecorator;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

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
					upstreamProducer.onConsumerError(new Exception("Test Exception"));
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
		assertEquals(CLOSED_WITH_ERROR, consumer.getUpstream().getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, producer.getProducerStatus());
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
		assertEquals(END_OF_STREAM, consumer.getUpstream().getProducerStatus());
		assertEquals(END_OF_STREAM, producer.getProducerStatus());
	}

	@Test
	public void testWithoutConsumer() {
		NioEventloop eventloop = new NioEventloop();

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList consumer = TestStreamConsumers.toListOneByOne(eventloop, list);
		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
		StreamProducerDecorator<Integer> producerDecorator = new StreamProducerDecorator<Integer>(eventloop, producer) {
		};
		StreamFunction<Integer, Integer> function = new StreamFunction<>(eventloop, Functions.<Integer>identity());

		producerDecorator.streamTo(function);
		eventloop.run();

		function.streamTo(consumer);
		eventloop.run();

		assertEquals(consumer.getList(), asList(1, 2, 3, 4, 5));
		assertEquals(END_OF_STREAM, consumer.getUpstream().getProducerStatus());
		assertEquals(END_OF_STREAM, producer.getProducerStatus());
	}
}
