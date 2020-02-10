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
import io.datakernel.datastream.*;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Represent {@link StreamTransformer} which receives data and saves it in collection, when it
 * receive end of stream it sorts it and streams to destination.
 *
 * @param <K> type of keys
 * @param <T> type of objects
 */
public final class StreamSorter<K, T> implements StreamTransformer<T, T> {
	private final AsyncCollector<? extends List<Integer>> temporaryStreamsCollector;
	private final StreamSorterStorage<T> storage;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final Comparator<T> itemComparator;
	private final boolean distinct;
	private final int itemsInMemory;

	private final Input input;
	private final StreamSupplier<T> output;
	private StreamConsumer<T> outputConsumer;

	// region creators
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

		this.output =
				new ForwardingStreamSupplier<T>(StreamSupplier.ofPromise(
						(this.temporaryStreamsCollector = AsyncCollector.create(new ArrayList<>()))
								.run(input.getEndOfStream())
								.get()
								.map(streamIds -> {
									input.list.sort(itemComparator);
									Iterator<T> iterator = !distinct ?
											input.list.iterator() :
											new DistinctIterator<>(input.list, keyFunction, keyComparator);
									StreamSupplier<T> listSupplier = StreamSupplier.ofIterator(iterator);
									if (streamIds.isEmpty()) {
										return listSupplier;
									} else {
										StreamMerger<K, T> streamMerger = StreamMerger.create(keyFunction, keyComparator, distinct);
										listSupplier.streamTo(streamMerger.newInput());
										streamIds.forEach(streamId ->
												StreamSupplier.ofPromise(storage.read(streamId))
														.streamTo(streamMerger.newInput()));
										return streamMerger
												.getOutput()
												.withLateBinding();
									}
								})
				)) {
					@Override
					public void setConsumer(@NotNull StreamConsumer<T> consumer) {
						super.setConsumer(consumer);
						outputConsumer = consumer;
					}
				};
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
	// endregion

	private final class Input extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private ArrayList<T> list = new ArrayList<>();

		@Override
		protected void onStarted() {
			getSupplier().resume(this);
		}

		@Override
		public void accept(T item) {
			list.add(item);
			if (list.size() >= itemsInMemory) {
				list.sort(itemComparator);
				Iterator<T> iterator = !distinct ?
						input.list.iterator() :
						new DistinctIterator<>(input.list, keyFunction, keyComparator);
				writeToTemporaryStorage(iterator)
						.whenResult($ -> suspendOrResume());
				suspendOrResume();
				list = new ArrayList<>(itemsInMemory);
			}
		}

		private Promise<Integer> writeToTemporaryStorage(Iterator<T> sortedList) {
			return temporaryStreamsCollector.addPromise(
					storage.newPartitionId()
							.then(partitionId -> storage.write(partitionId)
									.then(consumer -> StreamSupplier.ofIterator(sortedList).streamTo(consumer)
											.map($ -> partitionId))),
					List::add);
		}

		private void suspendOrResume() {
			if (temporaryStreamsCollector.getActivePromises() > 2) {
				getSupplier().suspend();
			} else {
				getSupplier().resume(this);
			}
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			return outputConsumer.getAcknowledgement();
		}

		@Override
		protected void onError(Throwable e) {
			// do nothing
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
