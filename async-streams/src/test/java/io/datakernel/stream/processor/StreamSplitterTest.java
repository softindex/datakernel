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

import static io.datakernel.stream.StreamStatus.*;
import static io.datakernel.stream.processor.Utils.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class StreamSplitterTest {
	@Test
	public void test1() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamSplitter<Integer> streamConcat = new StreamSplitter<>(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList1 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList2 = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(streamConcat);
		streamConcat.newOutput().streamTo(consumerToList1);
		streamConcat.newOutput().streamTo(consumerToList2);
		eventloop.run();
		assertEquals(asList(1, 2, 3), consumerToList1.getList());
		assertEquals(asList(1, 2, 3), consumerToList2.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, streamConcat.getUpstreamConsumer().getConsumerStatus());
		assertProducerStatuses(END_OF_STREAM, streamConcat.getDownstreamProducers());
	}

	@Test
	public void testConsumerDisconnectWithError() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
		StreamSplitter<Integer> streamConcat = new StreamSplitter<>(eventloop);

		List<Integer> toList1 = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList1 = TestStreamConsumers.toListOneByOne(eventloop, toList1);
		List<Integer> toList2 = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList2 = TestStreamConsumers.toListOneByOne(eventloop, toList2);

		List<Integer> toBadList = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> badConsumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, toBadList) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 3) {
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

		source.streamTo(streamConcat);

		streamConcat.newOutput().streamTo(consumerToList1);
		streamConcat.newOutput().streamTo(badConsumer);
		streamConcat.newOutput().streamTo(consumerToList2);

		eventloop.run();

		assertTrue(toList1.size() == 3);
		assertTrue(toList2.size() == 3);
		assertTrue(toBadList.size() == 3);

		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, streamConcat.getUpstreamConsumer().getConsumerStatus());
		assertProducerStatuses(CLOSED_WITH_ERROR, streamConcat.getDownstreamProducers());
	}

	@Test
	public void testProducerDisconnectWithError() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 1),
				StreamProducers.ofValue(eventloop, 2),
				StreamProducers.ofValue(eventloop, 3),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception("Test Exception"))
		);

		StreamSplitter<Integer> streamConcat = new StreamSplitter<>(eventloop);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumer<Integer> consumer1 = TestStreamConsumers.toListOneByOne(eventloop, list1);
		List<Integer> list2 = new ArrayList<>();
		StreamConsumer<Integer> consumer2 = TestStreamConsumers.toListOneByOne(eventloop, list2);
		List<Integer> list3 = new ArrayList<>();
		StreamConsumer<Integer> consumer3 = TestStreamConsumers.toListOneByOne(eventloop, list3);

		source.streamTo(streamConcat);
		streamConcat.newOutput().streamTo(consumer1);
		streamConcat.newOutput().streamTo(consumer2);
		streamConcat.newOutput().streamTo(consumer3);

		eventloop.run();

		assertTrue(list1.size() == 3);
		assertTrue(list2.size() == 3);
		assertTrue(list3.size() == 3);

		assertEquals(CLOSED_WITH_ERROR, streamConcat.getUpstreamConsumer().getConsumerStatus());
		assertProducerStatuses(CLOSED_WITH_ERROR, streamConcat.getDownstreamProducers());
	}
}