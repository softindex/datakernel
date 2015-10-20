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
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.examples.ScheduledProducer;
import io.datakernel.stream.processor.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.stream.StreamStatus.*;
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

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
	}

	@Test
	public void testWithoutConsumer() {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);

		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(sorter);
		eventloop.run();

		sorter.getSortedStream().streamTo(consumerToList);
		eventloop.run();

		storage.cleanup();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
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

		iterableSource.streamTo(sorter2);
		scheduledSource.streamTo(sorter1);

		sorter1.getSortedStream().streamTo(consumerToList1);
		sorter2.getSortedStream().streamTo(consumerToList2);

		eventloop.run();
		storage.cleanup();

		assertEquals(consumerToList1.getList(), asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
		assertEquals(consumerToList2.getList(), asList(10, 20, 30, 40, 50));

		assertEquals(END_OF_STREAM, iterableSource.getProducerStatus());
		assertEquals(END_OF_STREAM, scheduledSource.getProducerStatus());

		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter1.getSortedStream()).getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter1.getSortedStream()).getConsumerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter2.getSortedStream()).getProducerStatus());
		assertEquals(END_OF_STREAM, ((StreamForwarder) sorter2.getSortedStream()).getConsumerStatus());
	}

	@Test
	public void testErrorOnSorter() {
		NioEventloop eventloop = new NioEventloop();
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorterWithError sorter = new StreamSorterWithError(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);
		final List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop, list);

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());

		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
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

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
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

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 2);
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
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

		source.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertTrue(sorter.getItems() == 4);

		assertEquals(CLOSED_WITH_ERROR, consumerToList.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, ((StreamForwarder) sorter.getSortedStream()).getConsumerStatus());
	}

	public final class StreamSorterWithError extends AbstractStreamConsumer<Integer> implements StreamDataReceiver<Integer>, StreamSorterMBean {

		private final StreamMergeSorterStorage<Integer> storage;
		private final Function<Integer, Integer> keyFunction;
		private final Comparator<Integer> keyComparator;
		private final boolean deduplicate;
		protected final int itemsInMemorySize;

		private final Comparator<Integer> itemComparator;

		protected ArrayList<Integer> list;
		private List<Integer> listOfPartitions;

		private boolean writing;

		private StreamForwarder<Integer> downstream;

		protected long jmxItems;

		/**
		 * Creates a new instance of StreamSorter
		 *
		 * @param eventloop         event loop in which StreamSorter will run
		 * @param storage           storage for storing elements which was no placed to RAM
		 * @param keyFunction       function for searching key
		 * @param keyComparator     comparator for comparing key
		 * @param deduplicate       if it is true it means that in result will be not objects with same key
		 * @param itemsInMemorySize size of elements which can be saved in RAM before sorting
		 */
		public StreamSorterWithError(Eventloop eventloop, StreamMergeSorterStorage<Integer> storage,
		                             final Function<Integer, Integer> keyFunction, final Comparator<Integer> keyComparator, boolean deduplicate,
		                             int itemsInMemorySize) {
			super(eventloop);
			this.storage = checkNotNull(storage);
			this.keyComparator = checkNotNull(keyComparator);
			this.keyFunction = checkNotNull(keyFunction);
			this.deduplicate = deduplicate;
			checkArgument(itemsInMemorySize > 0, "itemsInMemorySize must be positive value, got %s", itemsInMemorySize);
			this.itemsInMemorySize = itemsInMemorySize;

			this.itemComparator = new Comparator<Integer>() {
				private final Function<Integer, Integer> _keyFunction = keyFunction;
				private final Comparator<Integer> _keyComparator = keyComparator;

				@Override
				public int compare(Integer item1, Integer item2) {
					Integer key1 = _keyFunction.apply(item1);
					Integer key2 = _keyFunction.apply(item2);
					return _keyComparator.compare(key1, key2);
				}
			};
			this.list = new ArrayList<>(itemsInMemorySize + (itemsInMemorySize >> 4));
			this.listOfPartitions = new ArrayList<>();
			this.downstream = new StreamForwarder<>(eventloop);
		}

		public StreamProducer<Integer> getSortedStream() {
			return downstream;
		}

		@Override
		public StreamDataReceiver<Integer> getDataReceiver() {
			return this;
		}

		/**
		 * Adds received data to storage, checks if its count bigger than itemsInMemorySize, if it is
		 * streams it to storage
		 *
		 * @param value received value
		 */
		@SuppressWarnings("AssertWithSideEffects")
		@Override
		public void onData(Integer value) {
			assert jmxItems != ++jmxItems;
			if (value == 5) {
				closeWithError(new Exception());
				return;
			}
			list.add(value);
			if (list.size() >= itemsInMemorySize) {
				nextState();
			}
		}

		protected void nextState() {
			// TODO (vsavchuk) Fix this
//		if (downstream.g() != null) {
//			return;
//		}
//
			boolean bufferFull = list.size() >= itemsInMemorySize;

			if (writing) {
				if (bufferFull) {
					suspend();
				}
				return;
			}

			if (getConsumerStatus() == END_OF_STREAM) {
				final StreamMerger<Integer, Integer> merger = StreamMerger.streamMerger(eventloop, keyFunction, keyComparator, deduplicate);

				Collections.sort(list, itemComparator);
				StreamProducer<Integer> queueProducer = StreamProducers.ofIterable(eventloop, list);
				list = null;

				queueProducer.streamTo(merger.newInput());

				for (int partition : listOfPartitions) {
					storage.read(partition).streamTo(merger.newInput());
				}

				merger.streamTo(downstream);
				return;
			}

			if (bufferFull) {
				Collections.sort(list, itemComparator);
				writing = true;
				listOfPartitions.add(storage.nextPartition());
				storage.write(StreamProducers.ofIterable(eventloop, list), new CompletionCallback() {
					@Override
					public void onComplete() {
						eventloop.post(new Runnable() {
							@Override
							public void run() {
								writing = false;
								nextState();
							}
						});
					}

					@Override
					public void onException(Exception e) {
						new StreamProducers.ClosingWithError<Integer>(eventloop, e).streamTo(downstream);
						closeWithError(e);

					}
				});
				this.list = new ArrayList<>(list.size() + (list.size() >> 8));
				return;
			}

			resume();
		}

		@Override
		protected void onStarted() {
		}

		@Override
		protected void onEndOfStream() {
			nextState();
		}

		@Override
		protected void onError(Exception e) {
			StreamProducers.<Integer>closingWithError(eventloop, e).streamTo(downstream);
			closeWithError(e);
		}

		@Override
		public long getItems() {
			return jmxItems;
		}

		@SuppressWarnings("AssertWithSideEffects")
		@Override
		public String toString() {
			String items = "?";
			assert (items = "" + jmxItems) != null;
			return '{' + super.toString() + " items:" + items + '}';
		}
	}
}