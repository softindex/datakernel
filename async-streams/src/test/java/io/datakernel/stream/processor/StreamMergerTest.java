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
import io.datakernel.eventloop.EventloopStub;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamMergerTest {

	@Test
	public void testDeduplicate() {
		EventloopStub eventloop = new EventloopStub();
		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, Collections.<Integer>emptyList());
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(3, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = new StreamMerger<>(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(merger.newInput());
		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.streamTo(consumer);

		eventloop.run();
		assertEquals(asList(3, 4, 6, 7), consumer.getList());

		assertTrue(source0.getStatus() == StreamProducer.CLOSED);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED);
	}

	@Test
	public void testDuplicate() {
		EventloopStub eventloop = new EventloopStub();
		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, Collections.<Integer>emptyList());
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(3, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = new StreamMerger<>(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), false);

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(merger.newInput());
		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.streamTo(consumer);

		eventloop.run();
		assertEquals(asList(3, 3, 4, 6, 7), consumer.getList());

		assertTrue(source0.getStatus() == StreamProducer.CLOSED);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED);
	}

	@Test
	public void test() {
		EventloopStub eventloop = new EventloopStub();
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

		StreamMerger<Integer, DataItem1> merger = new StreamMerger<>(eventloop, new Function<DataItem1, Integer>() {
			@Override
			public Integer apply(DataItem1 input) {
				return input.key2;
			}
		}, Ordering.<Integer>natural(), false);

		StreamConsumers.ToList<DataItem1> consumer = StreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.streamTo(consumer);

		eventloop.run();

		assertEquals(asList(d0, //DataItem1(0,1,1,1)
				d3, //DataItem1(1,1,1,4)
				d1, //DataItem1(0,2,1,2)
				d4, //DataItem1(1,5,1,5)
				d2  //DataItem1(0,6,1,3)
		), consumer.getList());

		assertTrue(source1.getStatus() == StreamProducer.CLOSED);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED);
	}

	@Test
	public void testDeduplicateEndOfStream() {
		EventloopStub eventloop = new EventloopStub();
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(7, 8, 3));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = new StreamMerger<>(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (item == 8) {
					onEndOfStream();
					return;
				}
				upstreamProducer.suspend();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.resume();
					}
				});
			}
		};

		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 5);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED);
	}

	@Test
	public void testDeduplicateWithError() {
		EventloopStub eventloop = new EventloopStub();
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(7, 8, 3));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));

		StreamMerger<Integer, Integer> merger = new StreamMerger<>(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumer = new StreamConsumers.ToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (item == 8) {
					onError(new Exception());
					return;
				}
				upstreamProducer.suspend();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.resume();
					}
				});
			}
		};

		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 5);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
//		source2.getStatus() should be equals to CLOSE?
//		assertTrue(source2.getStatus() == CLOSED_WITH_ERROR);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProducerDeduplicateWithError() {
		EventloopStub eventloop = new EventloopStub();
		StreamProducer<Integer> source1 = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 7),
				StreamProducers.ofValue(eventloop, 8),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()),
				StreamProducers.ofValue(eventloop, 3),
				StreamProducers.ofValue(eventloop, 9)
		);

		StreamProducer<Integer> source2 = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, 3),
				StreamProducers.ofValue(eventloop, 4),
				StreamProducers.ofValue(eventloop, 6),
				StreamProducers.ofValue(eventloop, 9)
		);

		StreamMerger<Integer, Integer> merger = new StreamMerger<>(eventloop, Functions.<Integer>identity(), Ordering.<Integer>natural(), true);

		List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList consumer = StreamConsumers.toListOneByOne(eventloop, list);

		source1.streamTo(merger.newInput());
		source2.streamTo(merger.newInput());

		merger.streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 0);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
	}

}
