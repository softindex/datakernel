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

package io.datakernel.aggregation;

import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.async.process.AsyncCollector;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.datastream.*;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public final class AggregationChunker<C, T> extends ForwardingStreamConsumer<T> {
	private final StreamConsumerSwitcher<T> switcher;
	private final SettablePromise<List<AggregationChunk>> result = new SettablePromise<>();

	private final AggregationStructure aggregation;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final PartitionPredicate<T> partitionPredicate;
	private final AggregationChunkStorage<C> storage;
	private final AsyncCollector<? extends List<AggregationChunk>> chunksCollector;
	private final DefiningClassLoader classLoader;

	private final int chunkSize;

	private AggregationChunker(StreamConsumerSwitcher<T> switcher,
			AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
			AggregationChunkStorage<C> storage,
			DefiningClassLoader classLoader,
			int chunkSize) {
		super(switcher);
		this.switcher = switcher;
		this.aggregation = aggregation;
		this.fields = fields;
		this.recordClass = recordClass;
		this.partitionPredicate = partitionPredicate;
		this.storage = storage;
		this.classLoader = classLoader;
		(this.chunksCollector = AsyncCollector.create(new ArrayList<>()))
				.run(switcher.getAcknowledgement());
		this.chunkSize = chunkSize;
		chunksCollector.get().whenComplete(result::trySet);
		getAcknowledgement().whenException(result::trySetException);
	}

	public static <C, T> AggregationChunker<C, T> create(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
			AggregationChunkStorage<C> storage,
			DefiningClassLoader classLoader,
			int chunkSize) {

		StreamConsumerSwitcher<T> switcher = StreamConsumerSwitcher.create();
		AggregationChunker<C, T> chunker = new AggregationChunker<>(switcher, aggregation, fields, recordClass, partitionPredicate, storage, classLoader, chunkSize);
		chunker.startNewChunk();
		return chunker;
	}

	public Promise<List<AggregationChunk>> getResult() {
		return result;
	}

	private class ChunkWriter extends ForwardingStreamConsumer<T> implements StreamDataAcceptor<T> {
		private final SettablePromise<AggregationChunk> result = new SettablePromise<>();
		private final int chunkSize;
		private final PartitionPredicate<T> partitionPredicate;
		private StreamDataAcceptor<T> dataAcceptor;

		private T first;
		private T last;
		private int count;

		boolean switched;

		public ChunkWriter(StreamConsumer<T> actualConsumer,
				C chunkId, int chunkSize, PartitionPredicate<T> partitionPredicate) {
			super(actualConsumer);
			this.chunkSize = chunkSize;
			this.partitionPredicate = partitionPredicate;
			actualConsumer.getAcknowledgement()
					.map($ -> count == 0 ?
							null :
							AggregationChunk.create(chunkId,
									fields,
									PrimaryKey.ofObject(first, aggregation.getKeys()),
									PrimaryKey.ofObject(last, aggregation.getKeys()),
									count))
					.whenComplete(result::trySet);
			getAcknowledgement().whenException(result::trySetException);
		}

		@Override
		public void setSupplier(@NotNull StreamSupplier<T> supplier) {
			super.setSupplier(new ForwardingStreamSupplier<T>(supplier) {
				@Override
				public void resume(@NotNull StreamDataAcceptor<T> dataAcceptor) {
					ChunkWriter.this.dataAcceptor = dataAcceptor;
					super.resume(ChunkWriter.this);
				}
			});
		}

		@Override
		public void accept(T item) {
			if (first == null) {
				first = item;
			}
			last = item;
			dataAcceptor.accept(item);
			if (++count == chunkSize || (partitionPredicate != null && !partitionPredicate.isSamePartition(last, item))) {
				if (!switched) {
					switched = true;
					startNewChunk();
				}
			}
		}

		public Promise<AggregationChunk> getResult() {
			return result;
		}
	}

	private void startNewChunk() {
		StreamConsumer<T> consumer = StreamConsumer.ofPromise(
				storage.createId()
						.then(chunkId -> storage.write(aggregation, fields, recordClass, chunkId, classLoader)
								.map(streamConsumer -> {
									ChunkWriter chunkWriter = new ChunkWriter(streamConsumer, chunkId, chunkSize, partitionPredicate);

									chunksCollector.addPromise(
											chunkWriter.getResult(),
											(accumulator, newChunk) -> {
												if (newChunk != null && newChunk.getCount() != 0) {
													accumulator.add(newChunk);
												}
											});

									return chunkWriter.withLateBinding();
								})));

		switcher.switchTo(consumer);
	}

}
