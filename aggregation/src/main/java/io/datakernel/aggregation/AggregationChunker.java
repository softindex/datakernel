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
import io.datakernel.async.StagesAccumulator;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.stream.StreamConsumerDecorator;
import io.datakernel.stream.StreamConsumerSwitcher;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamDataReceiver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.SettableStage.mirrorOf;
import static io.datakernel.async.Stages.onError;

public final class AggregationChunker<T> extends StreamConsumerDecorator<T> implements StreamConsumerWithResult<T, List<AggregationChunk>> {
	private final StreamConsumerSwitcher<T> switcher;
	private final SettableStage<List<AggregationChunk>> result;

	private final AggregationStructure aggregation;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final PartitionPredicate<T> partitionPredicate;
	private final AggregationChunkStorage storage;
	private final StagesAccumulator<List<AggregationChunk>> chunksAccumulator;
	private final DefiningClassLoader classLoader;

	private final int chunkSize;

	private AggregationChunker(StreamConsumerSwitcher<T> switcher,
	                           AggregationStructure aggregation, List<String> fields,
	                           Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
	                           AggregationChunkStorage storage,
	                           DefiningClassLoader classLoader,
	                           int chunkSize) {
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
		this.result = mirrorOf(chunksAccumulator.get());
		getEndOfStream().whenComplete(onError(result::trySetException));
	}

	public static <T> AggregationChunker<T> create(AggregationStructure aggregation, List<String> fields,
	                                               Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
	                                               AggregationChunkStorage storage,
	                                               DefiningClassLoader classLoader,
	                                               int chunkSize) {
		StreamConsumerSwitcher<T> switcher = StreamConsumerSwitcher.create();
		AggregationChunker<T> chunker = new AggregationChunker<>(switcher, aggregation, fields, recordClass, partitionPredicate, storage, classLoader, chunkSize);
		chunker.setActualConsumer(switcher);
		chunker.startNewChunk();
		return chunker;
	}

	@Override
	public CompletionStage<List<AggregationChunk>> getResult() {
		return result;
	}

	private class ChunkWriter extends StreamConsumerDecorator<T> implements StreamConsumerWithResult<T, AggregationChunk>, StreamDataReceiver<T> {
		private final SettableStage<AggregationChunk> result;
		private final long chunkId;
		private final int chunkSize;
		private final PartitionPredicate<T> partitionPredicate;
		private StreamDataReceiver<T> dataReceiver;

		private T first;
		private T last;
		private int count;

		boolean switched;

		public ChunkWriter(StreamConsumerWithResult<T, Void> actualConsumer,
		                   long chunkId, int chunkSize, PartitionPredicate<T> partitionPredicate) {
			super(actualConsumer);
			this.chunkId = chunkId;
			this.chunkSize = chunkSize;
			this.partitionPredicate = partitionPredicate;
			this.result = mirrorOf(actualConsumer.getResult()
					.thenApply($ -> count == 0 ? null :
							AggregationChunk.create(chunkId,
									fields,
									PrimaryKey.ofObject(first, aggregation.getKeys()),
									PrimaryKey.ofObject(last, aggregation.getKeys()),
									count)));
			getEndOfStream().whenComplete(onError(result::trySetException));
		}

		@Override
		protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
			this.dataReceiver = dataReceiver;
			return this;
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
		public CompletionStage<AggregationChunk> getResult() {
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