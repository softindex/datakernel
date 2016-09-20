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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represent {@link StreamTransformer} which receives data and saves it in collection, when it
 * receive end of stream it sorts it and streams to destination.
 *
 * @param <K> type of keys
 * @param <T> type of objects
 */
public final class StreamSorter<K, T> implements StreamTransformer<T, T>, EventloopJmxMBean {
	protected long jmxItems;

	private final Eventloop eventloop;

	private InputConsumer inputConsumer;

	// region creators
	private StreamSorter(Eventloop eventloop, StreamMergeSorterStorage<T> storage,
	                     final Function<T, K> keyFunction, final Comparator<K> keyComparator, boolean deduplicate,
	                     int itemsInMemorySize) {
		checkArgument(itemsInMemorySize > 0, "itemsInMemorySize must be positive value, got %s", itemsInMemorySize);
		checkNotNull(keyComparator);
		checkNotNull(keyFunction);
		checkNotNull(storage);

		this.eventloop = eventloop;

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
				StreamMerger.create(eventloop, keyFunction, keyComparator, deduplicate), deduplicate);
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
	public static <K, T> StreamSorter<K, T> create(Eventloop eventloop, StreamMergeSorterStorage<T> storage,
	                                               final Function<T, K> keyFunction, final Comparator<K> keyComparator, boolean deduplicate,
	                                               int itemsInMemorySize) {
		return new StreamSorter<>(eventloop, storage, keyFunction, keyComparator, deduplicate, itemsInMemorySize);
	}
	// endregion

	private final class InputConsumer extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		private final StreamMergeSorterStorage<T> storage;
		private final Comparator<T> itemComparator;
		private final StreamMerger<K, T> merger;
		private List<Integer> listOfPartitions;
		private final int itemsInMemorySize;
		private ArrayList<T> list;
		private StreamForwarder<T> forwarder;
		private final boolean deduplicate;

		private boolean writing;
		private int readPartitions;

		protected InputConsumer(Eventloop eventloop, int itemsInMemorySize, Comparator<T> itemComparator,
		                        StreamMergeSorterStorage<T> storage, StreamMerger<K, T> merger, boolean deduplicate) {
			super(eventloop);
			this.itemsInMemorySize = itemsInMemorySize;
			this.itemComparator = itemComparator;
			this.storage = storage;
			this.merger = merger;
			this.list = new ArrayList<>();
			this.listOfPartitions = new ArrayList<>();
			this.forwarder = StreamForwarder.create(eventloop);
			this.deduplicate = deduplicate;
		}

		@Override
		protected void onError(Exception e) {
			StreamProducers.<T>closingWithError(eventloop, e).streamTo(merger.newInput());
			merger.getOutput().streamTo(forwarder.getInput());
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {
			assert jmxItems != ++jmxItems;
			list.add(item);
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

				if (!deduplicate && listOfPartitions.isEmpty()) {
					queueProducer.streamTo(forwarder.getInput());
					return;
				}

				queueProducer.streamTo(merger.newInput());

				for (int partition : listOfPartitions) {
					storage.read(partition, new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							++readPartitions;
							if (readPartitions == listOfPartitions.size()) {
								storage.cleanup(listOfPartitions);
							}
						}
					}).streamTo(merger.newInput());
				}
				merger.getOutput().streamTo(forwarder.getInput());

				return;
			}

			if (bufferFull) {
				Collections.sort(list, itemComparator);
				writing = true;
				int partition = storage.write(StreamProducers.ofIterable(eventloop, list), new CompletionCallback() {
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
				listOfPartitions.add(partition);
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

	@Override
	public StreamConsumer<T> getInput() {
		return inputConsumer;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return inputConsumer.forwarder.getOutput();
	}

	// jmx

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
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