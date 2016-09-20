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

import com.google.common.base.Function;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamProducers.concat;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamFunctionTest {

	@Test
	public void testFunction() {
		Eventloop eventloop = Eventloop.create();

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input * input;
			}
		});

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(streamFunction.getInput());
		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, streamFunction.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, streamFunction.getOutput().getProducerStatus());
		assertEquals(END_OF_STREAM, consumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testFunctionConsumerError() {
		Eventloop eventloop = Eventloop.create();

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input * input;
			}
		});

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (list.size() == 2) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
				suspend();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						resume();
					}
				});
			}
		};

		source1.streamTo(streamFunction.getInput());
		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4), list);

		assertEquals(CLOSED_WITH_ERROR, source1.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFunction.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFunction.getOutput().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testFunctionProducerError() {
		Eventloop eventloop = Eventloop.create();

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input * input;
			}
		});

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source1 = concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception")));

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop, list);

		source1.streamTo(streamFunction.getInput());
		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9, 16, 25, 36), list);

		assertEquals(CLOSED_WITH_ERROR, consumer.getUpstream().getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFunction.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamFunction.getOutput().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input * input;
			}
		});

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(streamFunction.getInput());
		eventloop.run();

		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, streamFunction.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, streamFunction.getOutput().getProducerStatus());
		assertEquals(END_OF_STREAM, consumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}
}
