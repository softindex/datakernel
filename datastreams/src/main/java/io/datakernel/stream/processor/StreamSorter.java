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
import io.datakernel.async.StagesAccumulator;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represent {@link StreamTransformer} which receives data and saves it in collection, when it
 * receive end of stream it sorts it and streams to destination.
 *
 * @param <K> type of keys
 * @param <T> type of objects
 */
public final class StreamSorter<K, T> implements HasInput<T>, HasOutput<T> {
	private final Eventloop eventloop;

	private final StagesAccumulator<List<Integer>> temporaryStreams = StagesAccumulator.create(new ArrayList<>());
	private final StreamSorterStorage<T> storage;
	private final Comparator<T> itemComparator;
	private final int itemsInMemory;

	private Input input;
	private StreamProducer<T> output;

	// region creators
	private StreamSorter(Eventloop eventloop, StreamSorterStorage<T> storage,
	                     final Function<T, K> keyFunction, final Comparator<K> keyComparator, boolean deduplicate,
	                     int itemsInMemory) {
		checkArgument(itemsInMemory > 0, "itemsInMemorySize must be positive value, got %s", itemsInMemory);
		checkNotNull(keyComparator);
		checkNotNull(keyFunction);
		checkNotNull(storage);

		this.eventloop = eventloop;

		this.itemsInMemory = itemsInMemory;
		this.itemComparator = (item1, item2) -> {
			K key1 = keyFunction.apply(item1);
			K key2 = keyFunction.apply(item2);
			return keyComparator.compare(key1, key2);
		};
		this.storage = storage;

		this.input = new Input(eventloop);

		this.temporaryStreams.addStage(input.getCompletionStage(), (accumulator, $) -> accumulator);
		CompletionStage<StreamProducer<T>> outputStreamStage = this.temporaryStreams.get()
				.thenApply(streamIds -> {
					input.list.sort(itemComparator);
					StreamProducer<T> listProducer = StreamProducers.ofIterable(eventloop, input.list);
					if (streamIds.isEmpty()) {
						return listProducer;
					} else {
						StreamMerger<K, T> streamMerger = StreamMerger.create(eventloop, keyFunction, keyComparator, deduplicate);
						listProducer.streamTo(streamMerger.newInput());
						streamIds.forEach(streamId -> StreamProducerWithResult.ofStage(storage.read(streamId)).streamTo(streamMerger.newInput()));
						return streamMerger.getOutput();
					}
				});
		this.output = StreamProducers.ofStage(outputStreamStage);
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
	public static <K, T> StreamSorter<K, T> create(Eventloop eventloop, StreamSorterStorage<T> storage,
	                                               final Function<T, K> keyFunction, final Comparator<K> keyComparator, boolean deduplicate,
	                                               int itemsInMemorySize) {
		return new StreamSorter<>(eventloop, storage, keyFunction, keyComparator, deduplicate, itemsInMemorySize);
	}
	// endregion

	private final class Input extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		private ArrayList<T> list = new ArrayList<>();

		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@Override
		public void onData(T item) {
			list.add(item);
			if (list.size() >= itemsInMemory) {
				list.sort(itemComparator);
				writeToTemporaryStorage(list).thenAccept($ -> suspendOrResume());
				suspendOrResume();
				list = new ArrayList<>(itemsInMemory);
			}
		}

		private CompletionStage<Integer> writeToTemporaryStorage(List<T> sortedList) {
			return temporaryStreams.addStage(
					storage.write()
							.thenCompose(consumer -> {
								StreamProducer<T> producer = StreamProducers.ofIterable(eventloop, sortedList);
								producer.streamTo(consumer);
								return consumer.getResult();
							}),
					(accumulator, streamId) -> {
						accumulator.add(streamId);
						return accumulator;
					});
		}

		private void suspendOrResume() {
			if (temporaryStreams.getActiveStages() > 2) {
				getProducer().suspend();
			} else {
				getProducer().produce(this);
			}
		}

		@Override
		protected void onEndOfStream() {
			// do nothing
		}

		@Override
		protected void onError(Exception e) {
			// do nothing
		}
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

}