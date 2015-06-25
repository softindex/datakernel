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
import com.google.common.collect.Ordering;
import io.datakernel.eventloop.EventloopStub;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.examples.ScheduledProducer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSorterTest {
	@Test
	public void test() throws Exception {
		EventloopStub eventloop = new EventloopStub();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertTrue(source.getStatus() == StreamProducer.CLOSED);
		assertTrue(sorter.getSortedStream().getStatus() == StreamProducer.CLOSED);
//		assertNull(source.getWiredConsumerStatus());
	}

	@Test
	public void testCollision() {
		NioEventloop eventloop = new NioEventloop();
		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);

		final StreamProducer<Integer> scheduledSource = new ScheduledProducer(eventloop) {
			@Override
			public void scheduleNext() {
				if (numberToSend > 9) abort();
				if (scheduledRunnable != null && status >= END_OF_STREAM)
					return;
				if (numberToSend >= 5) {
					scheduledRunnable = eventloop.schedule(eventloop.currentTimeMillis() + 1000L, new Runnable() {
						@Override
						public void run() {
							send(numberToSend++);
							scheduleNext();
						}
					});
				} else {
					send(numberToSend++);
					scheduleNext();
				}
			}
		};
		StreamProducer<Integer> iterableSource = StreamProducers.ofIterable(eventloop, asList(30, 10, 30, 20, 50, 10, 40, 30, 20));

		StreamSorter<Integer, Integer> sorter1 = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);
		StreamSorter<Integer, Integer> sorter2 = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList1 = StreamConsumers.toListRandomlySuspending(eventloop, list1);
		List<Integer> list2 = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList2 = StreamConsumers.toListRandomlySuspending(eventloop, list2);

		iterableSource.streamTo(sorter2);
		scheduledSource.streamTo(sorter1);

		sorter1.getSortedStream().streamTo(consumerToList1);
		sorter2.getSortedStream().streamTo(consumerToList2);

		eventloop.run();
		storage.cleanup();

		assertEquals(consumerToList1.getList(), asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
		assertEquals(consumerToList2.getList(), asList(10, 20, 30, 40, 50));

		assertTrue(iterableSource.getStatus() == StreamProducer.CLOSED);
		assertTrue(scheduledSource.getStatus() == StreamProducer.CLOSED);
		assertTrue(sorter1.getSortedStream().getStatus() == StreamProducer.CLOSED);
		assertTrue(sorter2.getSortedStream().getStatus() == StreamProducer.CLOSED);
	}

	@Test
	public void testErrorOnSorter() {
		EventloopStub eventloop = new EventloopStub();
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<Integer, Integer>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2) {
			@SuppressWarnings("AssertWithSideEffects")
			@Override
			public void onData(Integer value) {
				assert jmxItems != ++jmxItems;
				if (value == 5) {
					onError(new Exception());
					return;
				}
				list.add(value);
				if (list.size() >= itemsInMemorySize) {
					nextState();
				}
			}
		};
		final List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop, list);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertTrue(sorter.getSortedStream().getStatus() == StreamProducer.CLOSED_WITH_ERROR);
		assertTrue(source.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
	}

	@Test
	public void testBadStorage() {
		EventloopStub eventloop = new EventloopStub();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<Integer>(eventloop) {
			@Override
			public StreamConsumer streamWriter() {
				final List<Integer> list = new ArrayList<>();
				storage.put(partition++, list);
				return new StreamConsumers.ToList<Integer>(eventloop, list) {
					@Override
					public void onData(Integer item) {
						super.onData(item);
						if (list.size() == 2) {
							onError(new Exception());
						}
					}
				};
			}
		};
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toListRandomlySuspending(eventloop, list);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertTrue(source.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
		assertTrue(sorter.getSortedStream().getStatus() == StreamProducer.CLOSED_WITH_ERROR);
	}

	@Test
	public void testErrorOnConsumer() {
		EventloopStub eventloop = new EventloopStub();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList = new StreamConsumers.ToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (list.size() == 2) {
					onError(new Exception());
				}
			}
		};

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 2);
		assertTrue(sorter.getSortedStream().getStatus() == StreamProducer.CLOSED_WITH_ERROR);
		assertTrue(source.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
	}

	@Test
	public void testErrorOnProducer() {
		EventloopStub eventloop = new EventloopStub();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception()),
				StreamProducers.ofIterable(eventloop, asList(5, 1, 4, 3, 2))
		);

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList = new StreamConsumers.ToList<>(eventloop, list);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertTrue(sorter.getItems() == 4);
		assertTrue(sorter.getSortedStream().getStatus() == StreamProducer.CLOSED_WITH_ERROR);
		assertTrue(source.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
	}
}