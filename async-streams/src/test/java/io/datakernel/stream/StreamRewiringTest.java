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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.StreamFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class StreamRewiringTest {
	@Test
	public void noRewire() {
		Eventloop eventloop = Eventloop.create();

		StreamProducers.OfIterator<Integer> producer = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator());
		StreamFunction<Integer, Integer> function1 = StreamFunction.create(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = StreamFunction.create(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = StreamFunction.create(eventloop, Functions.<String>identity());
		StreamConsumers.ToList<String> consumer1 = new StreamConsumers.ToList<>(eventloop);

		producer.streamTo(function1.getInput());
		function1.getOutput().streamTo(function2.getInput());
		function2.getOutput().streamTo(function3.getInput());
		function3.getOutput().streamTo(consumer1);

		eventloop.run();

		assertEquals(asList("1", "2", "3"), consumer1.getList());
		assertEquals(END_OF_STREAM, producer.getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void rewireConsumer1() {
		Eventloop eventloop = Eventloop.create();

		StreamProducers.OfIterator<Integer> producer = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator());
		StreamFunction<Integer, Integer> function1 = StreamFunction.create(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = StreamFunction.create(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = StreamFunction.create(eventloop, Functions.<String>identity());
		StreamConsumers.ToList<String> consumer1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<String> consumer2 = new StreamConsumers.ToList<>(eventloop);

		producer.streamTo(function1.getInput());
		function1.getOutput().streamTo(function2.getInput());
		function2.getOutput().streamTo(function3.getInput());
		function3.getOutput().streamTo(consumer1);

		function2.getOutput().streamTo(consumer2);

		eventloop.run();

		assertEquals(asList("1", "2", "3"), consumer2.getList());
		assertNotNull(consumer1.getUpstream());
		assertEquals(CLOSED_WITH_ERROR, consumer1.getUpstream().getProducerStatus());
		assertEquals(END_OF_STREAM, consumer2.getUpstream().getProducerStatus());
		assertEquals(END_OF_STREAM, producer.getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void rewireConsumer2() {
		Eventloop eventloop = Eventloop.create();

		StreamProducers.OfIterator<Integer> producer = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator());
		StreamFunction<Integer, Integer> function1 = StreamFunction.create(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = StreamFunction.create(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = StreamFunction.create(eventloop, Functions.<String>identity());
		StreamConsumers.ToList<String> consumer1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<String> consumer2 = new StreamConsumers.ToList<>(eventloop);

		producer.streamTo(function1.getInput());
		function1.getOutput().streamTo(function2.getInput());
		function2.getOutput().streamTo(function3.getInput());
		function3.getOutput().streamTo(consumer1);

		eventloop.run();
		assertEquals(asList("1", "2", "3"), consumer1.getList());

		function2.getOutput().streamTo(consumer2);

		eventloop.run();
		assertEquals(asList(), consumer2.getList());
		assertEquals(END_OF_STREAM, consumer2.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void rewireProducer1() {
		Eventloop eventloop = Eventloop.create();

		StreamProducers.OfIterator<Integer> producer0 = new StreamProducers.OfIterator<>(eventloop, asList(0).iterator(), false);
		StreamProducers.OfIterator<Integer> producer1 = new StreamProducers.OfIterator<>(eventloop, asList(1, 2, 3).iterator(), false);
		StreamProducers.OfIterator<Integer> producer2 = new StreamProducers.OfIterator<>(eventloop, asList(4, 5, 6).iterator());
		StreamProducers.OfIterator<Integer> producer3 = new StreamProducers.OfIterator<>(eventloop, asList(10).iterator());
		StreamFunction<Integer, Integer> function1 = StreamFunction.create(eventloop, Functions.<Integer>identity());
		StreamFunction<Integer, String> function2 = StreamFunction.create(eventloop, new Function<Integer, String>() {
			@Override
			public String apply(Integer input) {
				return input.toString();
			}
		});
		StreamFunction<String, String> function3 = StreamFunction.create(eventloop, Functions.<String>identity());
		List<String> resultList = new ArrayList<>();
		StreamConsumers.ToList<String> consumer = new StreamConsumers.ToList<>(eventloop, resultList);

		function1.getOutput().streamTo(function2.getInput());
		function2.getOutput().streamTo(function3.getInput());
		function3.getOutput().streamTo(consumer);

		function1.getInput().streamFrom(producer1);
		eventloop.run();
		assertEquals(asList("1", "2", "3"), resultList);

		function1.getInput().streamFrom(producer2);
		eventloop.run();
		assertEquals(asList("1", "2", "3", "4", "5", "6"), consumer.getList());

		function1.getInput().streamFrom(producer3);
		eventloop.run();
		assertEquals(asList("1", "2", "3", "4", "5", "6"), consumer.getList());

		assertEquals(END_OF_STREAM, producer2.getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testProducerWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();
		StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1));
		StreamFunction<Integer, Integer> function = StreamFunction.create(eventloop, Functions.<Integer>identity());
		producer.streamTo(function.getInput());

		eventloop.run();

		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<>(eventloop);
		function.getOutput().streamTo(consumer);

		eventloop.run();

		assertEquals(asList(1), consumer.getList());
		assertThat(eventloop, doesntHaveFatals());
	}
}