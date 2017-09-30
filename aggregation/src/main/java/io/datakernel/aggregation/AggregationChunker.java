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
import io.datakernel.async.StagesAccumulator;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public final class AggregationChunker<T> extends StreamConsumerDecorator<T, List<AggregationChunk>> {
	private final Eventloop eventloop;
	private final StreamConsumerSwitcher<T> switcher;

	private final AggregationStructure aggregation;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final PartitionPredicate<T> partitionPredicate;
	private final AggregationChunkStorage storage;
	private final StagesAccumulator<List<AggregationChunk>> chunksAccumulator;
	private final DefiningClassLoader classLoader;

	private int chunkSize = Aggregation.DEFAULT_CHUNK_SIZE;

	private AggregationChunker(Eventloop eventloop, StreamConsumerSwitcher<T> switcher,
	                           AggregationStructure aggregation, List<String> fields,
	                           Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
	                           AggregationChunkStorage storage,
	                           DefiningClassLoader classLoader) {
		this.eventloop = eventloop;
		this.switcher = switcher;
		this.aggregation = aggregation;
		this.fields = fields;
		this.recordClass = recordClass;
		this.partitionPredicate = partitionPredicate;
		this.storage = storage;
		this.classLoader = classLoader;
		this.chunksAccumulator = StagesAccumulator.<List<AggregationChunk>>create(new ArrayList<>())
				.withStage(switcher.getEndOfStream(), (accumulator, $) -> {});
	}

	public static <T> AggregationChunker<T> create(Eventloop eventloop,
	                                               AggregationStructure aggregation, List<String> fields,
	                                               Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
	                                               AggregationChunkStorage storage,
	                                               DefiningClassLoader classLoader) {
		StreamConsumerSwitcher<T> switcher = StreamConsumerSwitcher.create(eventloop);
		AggregationChunker<T> chunker = new AggregationChunker<>(eventloop, switcher, aggregation, fields, recordClass, partitionPredicate, storage, classLoader);
		chunker.setActualConsumer(switcher, chunker.chunksAccumulator.get());
		chunker.startNewChunk();
		return chunker;
	}

	public AggregationChunker<T> withChunkSize(int chunkSize) {
		setChunkSize(chunkSize);
		return this;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	private class ChunkWriter extends StreamConsumerDecorator<T, AggregationChunk> implements StreamDataReceiver<T> {
		private final CompletionStage<AggregationChunk> result;
		private final long chunkId;
		private final int chunkSize;
		private final PartitionPredicate<T> partitionPredicate;
		private StreamDataReceiver<T> dataReceiver;

		private T first;
		private T last;
		private int count;

		boolean switched;

		public ChunkWriter(StreamConsumerWithResult<T, ?> actualConsumer,
		                   long chunkId, int chunkSize, PartitionPredicate<T> partitionPredicate) {
			this.chunkId = chunkId;
			this.chunkSize = chunkSize;
			this.partitionPredicate = partitionPredicate;
			this.result = actualConsumer.getResult().thenApply($ -> count == 0 ? null :
					AggregationChunk.create(chunkId,
							fields,
							PrimaryKey.ofObject(first, aggregation.getKeys()),
							PrimaryKey.ofObject(last, aggregation.getKeys()),
							count));
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
	}

	private void startNewChunk() {
		StreamConsumerWithResult<T, AggregationChunk> consumer = StreamConsumers.ofStageWithResult(
				storage.createId().thenCompose(chunkId ->
						storage.write(aggregation, fields, recordClass, chunkId, classLoader).thenApply(streamConsumer -> {
							ChunkWriter chunkWriter = new ChunkWriter(streamConsumer, chunkId, chunkSize, partitionPredicate);
							chunkWriter.setActualConsumer(streamConsumer, chunkWriter.result);
							return chunkWriter;
						})));

		switcher.switchTo(consumer);

		chunksAccumulator.addStage(consumer.getResult(), (accumulator, newChunk) -> {
			if (newChunk != null && newChunk.getCount() != 0) {
				accumulator.add(newChunk);
			}
		});
	}

}