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

package io.datakernel.datastream.processor;

import io.datakernel.async.process.AsyncCollector;
import io.datakernel.datastream.AbstractStreamConsumer;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represent {@link StreamTransformer} which receives data and saves it in collection, when it
 * receive end of stream it sorts it and streams to destination.
 *
 * @param <K> type of keys
 * @param <T> type of objects
 */
public final class StreamSorter<K, T> implements StreamTransformer<T, T> {
	private static final Logger logger = getLogger(StreamSorter.class);
	private final AsyncCollector<? extends List<Integer>> temporaryStreamsCollector;
	private final StreamSorterStorage<T> storage;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final Comparator<T> itemComparator;
	private final boolean distinct;
	private final int itemsInMemory;

	private final Input input;
	private final StreamSupplier<T> output;

	private StreamSorter(StreamSorterStorage<T> storage,
	                     Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct,
	                     int itemsInMemory) {
		this.storage = storage;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.itemComparator = (item1, item2) -> {
			K key1 = keyFunction.apply(item1);
			K key2 = keyFunction.apply(item2);
			return keyComparator.compare(key1, key2);
		};
		this.distinct = distinct;
		this.itemsInMemory = itemsInMemory;

		this.input = new Input();

		List<Integer> partitionIds = new ArrayList<>();
		this.output = StreamSupplier.ofPromise(
				(this.temporaryStreamsCollector = AsyncCollector.create(partitionIds))
						.get()
						.map(streamIds -> {
							input.list.sort(itemComparator);
							Iterator<T> iterator = !distinct ?
									input.list.iterator() :
									new DistinctIterator<>(input.list, keyFunction, keyComparator);
							StreamSupplier<T> listSupplier = StreamSupplier.ofIterator(iterator);
							logger.info("Items in memory: {}, files: {}", input.list.size(), streamIds.size());
							if (streamIds.isEmpty()) {
								return listSupplier;
							} else {
								StreamMerger<K, T> streamMerger = StreamMerger.create(keyFunction, keyComparator, distinct);
								listSupplier.streamTo(streamMerger.newInput());
								for (Integer streamId : streamIds) {
									StreamSupplier.ofPromise(storage.read(streamId))
											.streamTo(streamMerger.newInput());
								}
								return streamMerger.getOutput();
							}
						}));
		this.output.getEndOfStream()
				.whenComplete(() -> {if (!partitionIds.isEmpty()) storage.cleanup(partitionIds);});
	}

	private static final class DistinctIterator<K, T> implements Iterator<T> {
		private final ArrayList<T> sortedList;
		private final Function<T, K> keyFunction;
		private final Comparator<K> keyComparator;
		int i = 0;

		private DistinctIterator(ArrayList<T> sortedList, Function<T, K> keyFunction, Comparator<K> keyComparator) {
			this.sortedList = sortedList;
			this.keyFunction = keyFunction;
			this.keyComparator = keyComparator;
		}

		@Override
		public boolean hasNext() {
			return i < sortedList.size();
		}

		@Override
		public T next() {
			T next = sortedList.get(i++);
			K nextKey = keyFunction.apply(next);
			while (i < sortedList.size()) {
				if (keyComparator.compare(nextKey, keyFunction.apply(sortedList.get(i))) == 0) {
					i++;
					continue;
				}
				break;
			}
			return next;
		}
	}

	/**
	 * Creates a new instance of StreamSorter
	 *
	 * @param storage           storage for storing elements which was no placed to RAM
	 * @param keyFunction       function for searching key
	 * @param keyComparator     comparator for comparing key
	 * @param distinct          if it is true it means that in result will be not objects with same key
	 * @param itemsInMemorySize size of elements which can be saved in RAM before sorting
	 */
	public static <K, T> StreamSorter<K, T> create(StreamSorterStorage<T> storage,
	                                               Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct,
	                                               int itemsInMemorySize) {
		return new StreamSorter<>(storage, keyFunction, keyComparator, distinct, itemsInMemorySize);
	}

	private final class Input extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private ArrayList<T> list = new ArrayList<>();

		@Override
		protected void onStarted() {
			resume(this);
		}

		@Override
		public void accept(T item) {
			list.add(item);
			if (list.size() >= itemsInMemory) {
				list.sort(itemComparator);
				Iterator<T> iterator = !distinct ?
						list.iterator() :
						new DistinctIterator<>(list, keyFunction, keyComparator);
				writeToTemporaryStorage(iterator)
						.whenResult(this::suspendOrResume)
						.whenException(this::closeEx);
				suspendOrResume();
				list = new ArrayList<>(itemsInMemory);
			}
		}

		private Promise<Integer> writeToTemporaryStorage(Iterator<T> sortedList) {
			return temporaryStreamsCollector.addPromise(
					storage.newPartitionId()
							.then(partitionId -> storage.write(partitionId)
									.then(consumer -> StreamSupplier.ofIterator(sortedList).streamTo(consumer))
									.map($ -> partitionId)),
					List::add);
		}

		private void suspendOrResume() {
			if (temporaryStreamsCollector.getActivePromises() > 2) {
				suspend();
			} else {
				resume(this);
			}
		}

		@Override
		protected void onEndOfStream() {
			temporaryStreamsCollector.run();
			output.getEndOfStream()
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onError(Throwable e) {
			temporaryStreamsCollector.closeEx(e);
		}

		@Override
		protected void onCleanup() {
			list = null;
		}
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

}
