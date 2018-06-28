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
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.StagesAccumulator;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.List;

public final class AggregationChunker<C, T> extends ForwardingStreamConsumer<T> implements StreamConsumerWithResult<T, List<AggregationChunk>> {
	private final StreamConsumerSwitcher<T> switcher;
	private final SettableStage<List<AggregationChunk>> result = SettableStage.create();

	private final AggregationStructure aggregation;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final PartitionPredicate<T> partitionPredicate;
	private final AggregationChunkStorage<C> storage;
	private final StagesAccumulator<List<AggregationChunk>> chunksAccumulator;
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
		this.chunksAccumulator = StagesAccumulator.<List<AggregationChunk>>create(new ArrayList<>())
				.withStage(switcher.getEndOfStream(), (accumulator, $) -> {});
		this.chunkSize = chunkSize;
		chunksAccumulator.get().whenComplete(result::trySet);
		getEndOfStream().whenException(result::trySetException);
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

	@Override
	public Stage<List<AggregationChunk>> getResult() {
		return result;
	}

	private class ChunkWriter extends ForwardingStreamConsumer<T> implements StreamConsumerWithResult<T, AggregationChunk>, StreamDataReceiver<T> {
		private final SettableStage<AggregationChunk> result = SettableStage.create();
		private final C chunkId;
		private final int chunkSize;
		private final PartitionPredicate<T> partitionPredicate;
		private StreamDataReceiver<T> dataReceiver;

		private T first;
		private T last;
		private int count;

		boolean switched;

		public ChunkWriter(StreamConsumerWithResult<T, Void> actualConsumer,
				C chunkId, int chunkSize, PartitionPredicate<T> partitionPredicate) {
			super(actualConsumer);
			this.chunkId = chunkId;
			this.chunkSize = chunkSize;
			this.partitionPredicate = partitionPredicate;
			actualConsumer.getResult()
					.thenApply($ -> count == 0 ?
							null :
							AggregationChunk.create(chunkId,
									fields,
									PrimaryKey.ofObject(first, aggregation.getKeys()),
									PrimaryKey.ofObject(last, aggregation.getKeys()),
									count))
					.whenComplete(result::trySet);
			getEndOfStream().whenException(result::trySetException);
		}

		@Override
		public void setProducer(StreamProducer<T> producer) {
			super.setProducer(new ForwardingStreamProducer<T>(producer) {
				@Override
				public void produce(StreamDataReceiver<T> dataReceiver) {
					ChunkWriter.this.dataReceiver = dataReceiver;
					super.produce(ChunkWriter.this);
				}
			});
		}

		@Override
		public void onData(T item) {
			if (first == null) {
				first = item;
			}
			last = item;
			dataReceiver.onData(item);
			if (++count == chunkSize || (partitionPredicate != null && !partitionPredicate.isSamePartition(last, item))) {
				if (!switched) {
					switched = true;
					startNewChunk();
				}
			}
		}

		@Override
		public Stage<AggregationChunk> getResult() {
			return result;
		}
	}

	private void startNewChunk() {
		StreamConsumerWithResult<T, AggregationChunk> consumer = StreamConsumerWithResult.ofStage(
				storage.createId()
						.thenCompose(chunkId -> storage.write(aggregation, fields, recordClass, chunkId, classLoader)
								.thenApply(streamConsumer ->
										new ChunkWriter(streamConsumer, chunkId, chunkSize, partitionPredicate)
												.withLateBinding())));

		switcher.switchTo(consumer);

		chunksAccumulator.addStage(consumer.getResult(), (accumulator, newChunk) -> {
			if (newChunk != null && newChunk.getCount() != 0) {
				accumulator.add(newChunk);
			}
		});
	}

}