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

package io.datakernel.stream;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.processor.StreamFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class StreamRewiringTest {
	@Test
	public void noRewire() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducers.OfIterator<Integer> producer = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator());
		StreamFunction<Integer, Integer> function1 = new StreamFunction<>(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = new StreamFunction<>(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = new StreamFunction<>(eventloop, Functions.<String>identity());
		StreamConsumers.ToList<String> consumer1 = new StreamConsumers.ToList<>(eventloop);

		producer.streamTo(function1);
		function1.streamTo(function2);
		function2.streamTo(function3);
		function3.streamTo(consumer1);

		eventloop.run();

		assertEquals(asList("1", "2", "3"), consumer1.getList());
		assertTrue(producer.getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void rewireConsumer1() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducers.OfIterator<Integer> producer = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator());
		StreamFunction<Integer, Integer> function1 = new StreamFunction<>(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = new StreamFunction<>(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = new StreamFunction<>(eventloop, Functions.<String>identity());
		StreamConsumers.ToList<String> consumer1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<String> consumer2 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumerSwitcher<String> switcher = new StreamConsumerSwitcher<>(eventloop);

		producer.streamTo(function1);
		function1.streamTo(function2);
		function2.streamTo(switcher);
		switcher.switchConsumerTo(function3);
		function3.streamTo(consumer1);

		switcher.switchConsumerTo(consumer2);

		eventloop.run();

		assertEquals(asList("1", "2", "3"), consumer2.getList());
		assertNotNull(consumer1.getUpstream());
		assertFalse(consumer1.getUpstreamStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(producer.getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}

	@Test
	public void rewireConsumer2() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducers.OfIterator<Integer> producer = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator());
		StreamFunction<Integer, Integer> function1 = new StreamFunction<>(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = new StreamFunction<>(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = new StreamFunction<>(eventloop, Functions.<String>identity());
		StreamConsumers.ToList<String> consumer1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<String> consumer2 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumerSwitcher<String> switcher = new StreamConsumerSwitcher<>(eventloop);

		producer.streamTo(function1);
		function1.streamTo(function2);
		function2.streamTo(switcher);
		switcher.switchConsumerTo(function3);
		function3.streamTo(consumer1);

		eventloop.run();
		assertEquals(asList("1", "2", "3"), consumer1.getList());

		switcher.switchConsumerTo(consumer2);

		eventloop.run();
//		assertEquals(asList(), consumer2.getResult()); FIXME
//		assertTrue(consumer2.getConsumerStatus().isComplete()); FIXME
	}

	@Test
	public void rewireProducer1() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducers.OfIterator<Integer> producer0 = new StreamProducers.OfIterator<>(eventloop, asList(0).iterator(), false);
		StreamProducers.OfIterator<Integer> producer1 = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator(), false);
		StreamProducers.OfIterator<Integer> producer2 = new StreamProducers.OfIterator<>(eventloop, asList(4, 5, 6).iterator());
		StreamProducers.OfIterator<Integer> producer3 = new StreamProducers.OfIterator<>(eventloop, asList(10).iterator());
		StreamFunction<Integer, Integer> function1 = new StreamFunction<>(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = new StreamFunction<>(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = new StreamFunction<>(eventloop, Functions.<String>identity());
		List<String> resultList = new ArrayList<>();
		StreamConsumers.ToList<String> consumer = new StreamConsumers.ToList<>(eventloop, resultList);

		StreamProducerSwitcher<Integer> switcher = new StreamProducerSwitcher<>(eventloop, producer0);

		switcher.streamTo(function1);
		function1.streamTo(function2);
		function2.streamTo(function3);
		function3.streamTo(consumer);

		switcher.switchProducerTo(producer1);
		eventloop.run();
		assertEquals(asList("1", "2", "3"), resultList);

		switcher.switchProducerTo(producer2);
		eventloop.run();
		assertEquals(asList("1", "2", "3", "4", "5", "6"), consumer.getList());

		switcher.switchProducerTo(producer3);
		eventloop.run();
		assertEquals(asList("1", "2", "3", "4", "5", "6"), consumer.getList());

//		assertNull(producer1.getWiredConsumerStatus());
		assertTrue(producer2.getStatus() == AbstractStreamProducer.END_OF_STREAM);
	}
}