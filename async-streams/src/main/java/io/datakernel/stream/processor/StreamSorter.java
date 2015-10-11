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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.stream.AbstractStreamConsumer.StreamConsumerStatus.END_OF_STREAM;

/**
 * Represent {@link AbstractStreamTransformer_1_1} which receives data and saves it in collection, when it
 * receive end of stream it sorts it and streams to destination.
 *
 * @param <K> type of keys
 * @param <T> type of objects
 */
public class StreamSorter<K, T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T>, StreamSorterMBean {

	private final StreamMergeSorterStorage<T> storage;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean deduplicate;
	protected final int itemsInMemorySize;

	private final Comparator<T> itemComparator;

	protected List<T> list;
	private List<Integer> listOfPartitions;

	private StreamProducer<T> saveProducer;

	private StreamForwarder<T> downstream;

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
	public StreamSorter(Eventloop eventloop, StreamMergeSorterStorage<T> storage,
	                    final Function<T, K> keyFunction, final Comparator<K> keyComparator, boolean deduplicate,
	                    int itemsInMemorySize) {
		super(eventloop);
		this.storage = checkNotNull(storage);
		this.keyComparator = checkNotNull(keyComparator);
		this.keyFunction = checkNotNull(keyFunction);
		this.deduplicate = deduplicate;
		checkArgument(itemsInMemorySize > 0, "itemsInMemorySize must be positive value, got %s", itemsInMemorySize);
		this.itemsInMemorySize = itemsInMemorySize;

		this.itemComparator = new Comparator<T>() {
			private final Function<T, K> _keyFunction = keyFunction;
			private final Comparator<K> _keyComparator = keyComparator;

			@Override
			public int compare(T item1, T item2) {
				K key1 = _keyFunction.apply(item1);
				K key2 = _keyFunction.apply(item2);
				return _keyComparator.compare(key1, key2);
			}
		};
		this.list = new ArrayList<>(itemsInMemorySize + (itemsInMemorySize >> 4));
		this.listOfPartitions = new ArrayList<>();
		this.downstream = new StreamForwarder<>(eventloop);
		this.downstream.addProducerCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				close();
			}

			@Override
			public void onException(Exception exception) {
				closeWithError(exception);
			}
		});
	}

	public StreamProducer<T> getSortedStream() {
		return downstream;
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
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
	public void onData(T value) {
		assert jmxItems != ++jmxItems;
		list.add(value);
		if (list.size() >= itemsInMemorySize) {
			nextState();
		}
	}

	protected void nextState() {
		// TODO (vsavchuk) Fix this
//		if (result.getUpstream() != null) {
//			return;
//		}

		boolean bufferFull = list.size() >= itemsInMemorySize;

		if (saveProducer != null) {
			if (bufferFull) {
				suspend();
			}
			return;
		}

		if (getStatus() == END_OF_STREAM) {
			final StreamMerger<K, T> merger = StreamMerger.streamMerger(eventloop, keyFunction, keyComparator, deduplicate);

			Collections.sort(list, itemComparator);
			StreamProducer<T> queueProducer = StreamProducers.ofIterable(eventloop, list);
			list = null;

			queueProducer.streamTo(merger.newInput());

			for (int partition : listOfPartitions) {
				storage.streamReader(partition).streamTo(merger.newInput());
			}

			merger.streamTo(downstream);
			return;
		}

		if (bufferFull) {
			Collections.sort(list, itemComparator);
			saveProducer = StreamProducers.ofIterable(eventloop, list);
			listOfPartitions.add(storage.nextPartition());
			saveProducer.streamTo(storage.streamWriter());
			saveProducer.addProducerCompletionCallback(new CompletionCallback() {
				@Override
				public void onComplete() {
					eventloop.post(new Runnable() {
						@Override
						public void run() {
							saveProducer = null;
							nextState();
						}
					});
				}

				@Override
				public void onException(Exception e) {
//					onConsumerError(e);
					new StreamProducers.ClosingWithError<T>(eventloop, e).streamTo(downstream);
				}
			});
			this.list = new ArrayList<>(list.size() + (list.size() >> 4));
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
		StreamProducers.<T>closingWithError(eventloop, e).streamTo(downstream);
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