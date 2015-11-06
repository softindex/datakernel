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

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.examples.ScheduledProducer;
import io.datakernel.stream.processor.StreamMergeSorterStorage;
import io.datakernel.stream.processor.StreamMergeSorterStorageStub;
import io.datakernel.stream.processor.StreamSorter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSorterTest {
	@Test
	public void test() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, sorter.getOutput().getProducerStatus());
		assertEquals(END_OF_STREAM, sorter.getInput().getConsumerStatus());
	}

	@Test
	public void testWithoutConsumer() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(sorter.getInput());
		eventloop.run();

		sorter.getOutput().streamTo(consumerToList);
		eventloop.run();

		storage.cleanup();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, sorter.getOutput().getProducerStatus());
		assertEquals(END_OF_STREAM, sorter.getInput().getConsumerStatus());
	}

	@Test
	public void testCollision() {
		NioEventloop eventloop = new NioEventloop();
		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);

		final StreamProducer<Integer> scheduledSource = new ScheduledProducer(eventloop) {
			@Override
			protected void onDataReceiverChanged() {

			}

			@Override
			public void scheduleNext() {
				if (numberToSend > 9) {
					abort();
					return;
				}
				if (scheduledRunnable != null && getProducerStatus().isClosed())
					return;
				if (numberToSend >= 5) {
					scheduledRunnable = eventloop.schedule(eventloop.currentTimeMillis() + 100L, new Runnable() {
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
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList1 = TestStreamConsumers.toListRandomlySuspending(eventloop, list1);
		List<Integer> list2 = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList2 = TestStreamConsumers.toListRandomlySuspending(eventloop, list2);

		iterableSource.streamTo(sorter2.getInput());
		scheduledSource.streamTo(sorter1.getInput());

		sorter1.getOutput().streamTo(consumerToList1);
		sorter2.getOutput().streamTo(consumerToList2);

		eventloop.run();
		storage.cleanup();

		assertEquals(consumerToList1.getList(), asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
		assertEquals(consumerToList2.getList(), asList(10, 20, 30, 40, 50));

		assertEquals(END_OF_STREAM, iterableSource.getProducerStatus());
		assertEquals(END_OF_STREAM, scheduledSource.getProducerStatus());

		assertEquals(END_OF_STREAM, sorter1.getOutput().getProducerStatus());
		assertEquals(END_OF_STREAM, sorter1.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, sorter2.getOutput().getProducerStatus());
		assertEquals(END_OF_STREAM, sorter2.getInput().getConsumerStatus());
	}

	@Test
	public void testBadStorage() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<Integer>(eventloop) {
			@Override
			public void write(StreamProducer<Integer> producer, final CompletionCallback completionCallback) {
				final List<Integer> list = new ArrayList<>();
				storage.put(partition++, list);
				TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
					@Override
					public void onData(Integer item) {
						list.add(item);
						if (list.size() == 2) {
							closeWithError(new Exception());
						}
					}
				};
				producer.streamTo(consumer);

				consumer.setCompletionCallback(completionCallback);
			}
		};
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop, list);

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getOutput().getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
	}

	@Test
	public void testErrorOnConsumer() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (list.size() == 2) {
					closeWithError(new Exception());
				}
			}
		};

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 2);
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getOutput().getProducerStatus());
		assertEquals(END_OF_STREAM, sorter.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
	}

	@Test
	public void testErrorOnProducer() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2)),
				StreamProducers.<Integer>closingWithError(eventloop, new Exception())
		);

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList = new StreamConsumers.ToList<>(eventloop, list);

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertTrue(sorter.getItems() == 4);

		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getOutput().getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getInput().getConsumerStatus());
	}
}