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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.Utils.assertProducerStatuses;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class StreamSharderTest {

	private static final Sharder<Integer> SHARDER = new Sharder<Integer>() {
		@Override
		public int shard(Integer object) {
			return object % 2;
		}
	};

	@Test
	public void test1() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamSharder<Integer, Integer> streamSharder = StreamSharder.create(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4));
		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, streamSharder.getInput().getConsumerStatus());
		assertProducerStatuses(END_OF_STREAM, streamSharder.getOutputs());
		assertEquals(END_OF_STREAM, consumer1.getConsumerStatus());
		assertEquals(END_OF_STREAM, consumer2.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void test2() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamSharder<Integer, Integer> streamSharder = StreamSharder.create(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4));
		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, streamSharder.getInput().getConsumerStatus());
		assertProducerStatuses(END_OF_STREAM, streamSharder.getOutputs());
		assertEquals(END_OF_STREAM, consumer1.getConsumerStatus());
		assertEquals(END_OF_STREAM, consumer2.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithError() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamSharder<Integer, Integer> streamSharder = StreamSharder.create(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4));

		List<Integer> list1 = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumer1 = new StreamConsumers.ToList<Integer>(eventloop, list1);

		List<Integer> list2 = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list2) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 3) {
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

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();

		assertTrue(list1.size() == 1);
		assertTrue(list2.size() == 2);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamSharder.getInput().getConsumerStatus());
		assertProducerStatuses(CLOSED_WITH_ERROR, streamSharder.getOutputs());
		assertEquals(CLOSED_WITH_ERROR, consumer1.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer2.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testProducerWithError() throws Exception {
		final Eventloop eventloop = Eventloop.create();

		StreamSharder<Integer, Integer> streamSharder = StreamSharder.create(eventloop, SHARDER, Functions.<Integer>identity());

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 1),
				StreamProducers.ofValue(eventloop, 2),
				StreamProducers.ofValue(eventloop, 3),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception"))
		);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumer<Integer> consumer1 = TestStreamConsumers.toListOneByOne(eventloop, list1);
		List<Integer> list2 = new ArrayList<>();
		StreamConsumer<Integer> consumer2 = TestStreamConsumers.toListOneByOne(eventloop, list2);

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);

		eventloop.run();

		assertTrue(list1.size() == 1);
		assertTrue(list2.size() == 2);

		assertEquals(CLOSED_WITH_ERROR, streamSharder.getInput().getConsumerStatus());
		assertProducerStatuses(CLOSED_WITH_ERROR, streamSharder.getOutputs());
		assertEquals(CLOSED_WITH_ERROR, consumer1.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer2.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}
}
