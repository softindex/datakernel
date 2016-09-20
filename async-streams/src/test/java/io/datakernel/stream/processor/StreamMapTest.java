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
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class StreamMapTest {

	private static final StreamMap.MapperProjection<Integer, Integer> FUNCTION = new StreamMap.MapperProjection<Integer, Integer>() {
		@Override
		protected Integer apply(Integer input) {
			return input + 10;
		}
	};

	@Test
	public void test1() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamMap<Integer, Integer> projection = StreamMap.create(eventloop, FUNCTION);
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(projection.getInput());
		projection.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(11, 12, 13), consumer.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, projection.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, projection.getOutput().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithError() throws Exception {
		Eventloop eventloop = Eventloop.create();
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamMap<Integer, Integer> projection = StreamMap.create(eventloop, FUNCTION);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 12) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
				upstreamProducer.onConsumerSuspended();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.onConsumerResumed();
					}
				});
			}
		};

		source.streamTo(projection.getInput());
		projection.getOutput().streamTo(consumer);

		eventloop.run();
		assertTrue(list.size() == 2);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, projection.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, projection.getOutput().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testProducerWithError() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 1),
				StreamProducers.ofValue(eventloop, 2),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception")));

		StreamMap<Integer, Integer> projection = StreamMap.create(eventloop, FUNCTION);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source.streamTo(projection.getInput());
		projection.getOutput().streamTo(consumer);

		eventloop.run();
		assertTrue(list.size() == 2);
		assertEquals(CLOSED_WITH_ERROR, consumer.getUpstream().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamMap<Integer, Integer> projection = StreamMap.create(eventloop, FUNCTION);
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(projection.getInput());
		eventloop.run();

		projection.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(11, 12, 13), consumer.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, projection.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, projection.getOutput().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}
}
