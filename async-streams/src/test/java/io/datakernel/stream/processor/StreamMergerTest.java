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
import com.google.common.base.Functions;
import com.google.common.collect.Ordering;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.StreamStatus;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.Utils.assertConsumerStatuses;
import static io.datakernel.stream.processor.Utils.consumerStatuses;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class StreamMergerTest {

	@Test
	public void testDeduplicate() {
		Eventloop eventloop = Eventloop.create();
		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, Collections.<Integer>emptyList());
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(3, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = StreamMerger.create(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(merger.newInput());
		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(3, 4, 6, 7), consumer.getList());

		assertEquals(END_OF_STREAM, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, consumer.getConsumerStatus());
		assertEquals(END_OF_STREAM, merger.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, merger.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testDuplicate() {
		Eventloop eventloop = Eventloop.create();
		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, Collections.<Integer>emptyList());
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(3, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = StreamMerger.create(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), false);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(merger.newInput());
		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(3, 3, 4, 6, 7), consumer.getList());

		assertEquals(END_OF_STREAM, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, consumer.getConsumerStatus());
		assertEquals(END_OF_STREAM, merger.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, merger.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void test() {
		Eventloop eventloop = Eventloop.create();
		DataItem1 d0 = new DataItem1(0, 1, 1, 1);
		DataItem1 d1 = new DataItem1(0, 2, 1, 2);
		DataItem1 d2 = new DataItem1(0, 6, 1, 3);
		DataItem1 d3 = new DataItem1(1, 1, 1, 4);
		DataItem1 d4 = new DataItem1(1, 5, 1, 5);

		StreamProducer<DataItem1> source1 = StreamProducers.ofIterable(eventloop,
				asList(d0, //DataItem1(0,1,1,1)
						d1, //DataItem1(0,2,1,2)
						d2  //DataItem1(0,6,1,3)
				));
		StreamProducer<DataItem1> source2 = StreamProducers.ofIterable(eventloop,
				asList(d3,//DataItem1(1,1,1,4)
						d4 //DataItem1(1,5,1,5)
				));

		StreamMerger<Integer, DataItem1> merger = StreamMerger.create(eventloop, new Function<DataItem1, Integer>() {
			@Override
			public Integer apply(DataItem1 input) {
				return input.key2;
			}
		}, Ordering.<Integer>natural(), false);

		TestStreamConsumers.TestConsumerToList<DataItem1> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.getOutput().streamTo(consumer);

		eventloop.run();

		assertEquals(asList(d0, //DataItem1(0,1,1,1)
				d3, //DataItem1(1,1,1,4)
				d1, //DataItem1(0,2,1,2)
				d4, //DataItem1(1,5,1,5)
				d2  //DataItem1(0,6,1,3)
		), consumer.getList());

		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, consumer.getConsumerStatus());
		assertEquals(END_OF_STREAM, merger.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, merger.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testDeduplicateWithError() {
		Eventloop eventloop = Eventloop.create();
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(7, 8, 3));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = StreamMerger.create(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 8) {
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

		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.getOutput().streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 5);
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, merger.getOutput().getProducerStatus());
		assertArrayEquals(new StreamStatus[]{END_OF_STREAM, END_OF_STREAM},
				consumerStatuses(merger.getInputs()));
		assertThat(eventloop, doesntHaveFatals());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProducerDeduplicateWithError() {
		Eventloop eventloop = Eventloop.create();
		StreamProducer<Integer> source1 = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 7),
				StreamProducers.ofValue(eventloop, 8),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception")),
				StreamProducers.ofValue(eventloop, 3),
				StreamProducers.ofValue(eventloop, 9)
		);
		StreamProducer<Integer> source2 = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 3),
				StreamProducers.ofValue(eventloop, 4),
				StreamProducers.ofValue(eventloop, 6),
				StreamProducers.ofValue(eventloop, 9)
		);

		StreamMerger<Integer, Integer> merger = StreamMerger.create(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.getOutput().streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, consumer.getUpstream().getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumer.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, merger.getOutput().getProducerStatus());
		assertArrayEquals(new StreamStatus[]{CLOSED_WITH_ERROR, END_OF_STREAM},
				consumerStatuses(merger.getInputs()));
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();
		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, Collections.<Integer>emptyList());
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(3, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = StreamMerger.create(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(merger.newInput());
		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());
		eventloop.run();

		merger.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(3, 4, 6, 7), consumer.getList());

		assertEquals(END_OF_STREAM, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, consumer.getConsumerStatus());
		assertEquals(END_OF_STREAM, merger.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, merger.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}
}
