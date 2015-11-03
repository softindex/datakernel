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
	public void testErrorOnSorter() {
		NioEventloop eventloop = new NioEventloop();
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorterWithError<Integer, Integer> sorter = new StreamSorterWithError<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 2);
		final List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop, list);

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();
		storage.cleanup();

		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, source.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, sorter.getOutput().getProducerStatus());

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

	// override onData(T item) with error
	public final class StreamSorterWithError<K, T> implements StreamTransformer<T, T>, StreamSorterMBean {
		protected long jmxItems;

		private InputConsumer inputConsumer;

		@Override
		public StreamConsumer<T> getInput() {
			return inputConsumer;
		}

		@Override
		public StreamProducer<T> getOutput() {
			return inputConsumer.merger.getOutput();
		}

		private final class InputConsumer extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
			private final StreamMergeSorterStorage<T> storage;
			protected final int itemsInMemorySize;
			private final Comparator<T> itemComparator;
			private List<Integer> listOfPartitions;
			protected ArrayList<T> list;
			private final StreamMerger<K, T> merger;
			private boolean writing;

			protected InputConsumer(Eventloop eventloop, int itemsInMemorySize, Comparator<T> itemComparator, StreamMergeSorterStorage<T> storage, StreamMerger<K, T> merger) {
				super(eventloop);
				this.itemsInMemorySize = itemsInMemorySize;
				this.itemComparator = itemComparator;
				this.storage = storage;
				this.merger = merger;
				this.list = new ArrayList<>(this.itemsInMemorySize + (this.itemsInMemorySize >> 4));
				this.listOfPartitions = new ArrayList<>();
			}

			@Override
			protected void onError(Exception e) {
				StreamProducers.<T>closingWithError(eventloop, e).streamTo(merger.newInput());
			}

			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(T value) {
				assert jmxItems != ++jmxItems;
				if (value instanceof Integer && ((Integer) value) == 5) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
				list.add(value);
				if (list.size() >= itemsInMemorySize) {
					nextState();
				}
			}

			private void nextState() {
				if (list == null) {
					return;
				}

				boolean bufferFull = list.size() >= itemsInMemorySize;

				if (writing) {
					if (bufferFull) {
						suspend();
					}
					return;
				}

				if (getConsumerStatus() == StreamStatus.END_OF_STREAM) {
					Collections.sort(list, itemComparator);
					StreamProducer<T> queueProducer = StreamProducers.ofIterable(eventloop, list);
					list = null;

					queueProducer.streamTo(merger.newInput());

					for (int partition : listOfPartitions) {
						storage.read(partition).streamTo(merger.newInput());
					}

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
							new StreamProducers.ClosingWithError<T>(eventloop, e).streamTo(merger.newInput());
							closeWithError(e);

						}
					});
					list = new ArrayList<>(list.size() + (list.size() >> 8));
					return;
				}

				resume();
			}

			@Override
			protected void onEndOfStream() {
				nextState();
			}

			@Override
			public void suspend() {
				super.suspend();
			}

			@Override
			public void resume() {
				super.resume();
			}

			@Override
			public void closeWithError(Exception e) {
				super.closeWithError(e);
			}
		}

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
		public StreamSorterWithError(Eventloop eventloop, StreamMergeSorterStorage<T> storage,
		                    final Function<T, K> keyFunction, final Comparator<K> keyComparator, boolean deduplicate,
		                    int itemsInMemorySize) {
			checkArgument(itemsInMemorySize > 0, "itemsInMemorySize must be positive value, got %s", itemsInMemorySize);
			checkNotNull(keyComparator);
			checkNotNull(keyFunction);
			checkNotNull(storage);

			Comparator<T> itemComparator = new Comparator<T>() {
				private final Function<T, K> _keyFunction = keyFunction;
				private final Comparator<K> _keyComparator = keyComparator;

				@Override
				public int compare(T item1, T item2) {
					K key1 = _keyFunction.apply(item1);
					K key2 = _keyFunction.apply(item2);
					return _keyComparator.compare(key1, key2);
				}
			};

			this.inputConsumer = new InputConsumer(eventloop, itemsInMemorySize, itemComparator, storage,
					StreamMerger.streamMerger(eventloop, keyFunction, keyComparator, deduplicate));
		}

		//for test only
		StreamStatus getUpstreamConsumerStatus() {
			return inputConsumer.getConsumerStatus();
		}

		// for test only
		StreamStatus getDownstreamProducerStatus() {
			return inputConsumer.merger.getOutput().getProducerStatus();
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