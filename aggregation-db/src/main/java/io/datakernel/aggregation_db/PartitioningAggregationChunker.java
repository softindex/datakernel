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

package io.datakernel.aggregation_db;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamStatus;
import io.datakernel.stream.processor.AbstractStreamSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class PartitioningAggregationChunker<T> extends AbstractStreamSplitter<T> {
	private static final Logger logger = LoggerFactory.getLogger(PartitioningAggregationChunker.class);

	private final PartitioningStrategy partitioningStrategy;

	private final Eventloop eventloop;
	private final List<String> keys;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final AggregationChunkStorage storage;
	private final AggregationMetadataStorage metadataStorage;
	private final int chunkSize;
	private final ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback;

	private final IntObjectMap<StreamDataReceiver<T>> partitionChunkers = new IntObjectOpenHashMap<>();
	private final List<AggregationChunk.NewChunk> chunks = new ArrayList<>();

	private boolean returnedResult;

	public PartitioningAggregationChunker(PartitioningStrategy partitioningStrategy, Eventloop eventloop,
	                                      List<String> keys, List<String> fields,
	                                      Class<T> recordClass, AggregationChunkStorage storage,
	                                      AggregationMetadataStorage metadataStorage, int chunkSize,
	                                      ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		super(eventloop);
		this.partitioningStrategy = partitioningStrategy;
		this.eventloop = eventloop;
		this.keys = keys;
		this.fields = fields;
		this.recordClass = recordClass;
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.chunkSize = chunkSize;
		this.chunksCallback = chunksCallback;
		this.inputConsumer = new InputConsumer() {
			@Override
			public void onData(T item) {
				dispatchItem(item);
			}

			@Override
			protected void onError(Exception e) {
				reportException(e);
			}
		};
	}

	private void dispatchItem(T item) {
		int partition = partitioningStrategy.getPartition(item);
		ensurePartitionChunker(partition).onData(item);
	}

	private StreamDataReceiver<T> ensurePartitionChunker(int partition) {
		StreamDataReceiver<T> chunker = partitionChunkers.get(partition);

		if (chunker != null)
			return chunker;

		AggregationChunker<T> newChunker = new AggregationChunker<>(eventloop, keys, fields, recordClass,
				storage, metadataStorage, chunkSize, getResultCallback(partition));

		addOutput(new OutputProducer<T>()).streamTo(newChunker);

		partitionChunkers.put(partition, newChunker.getDataReceiver());

		return newChunker.getDataReceiver();
	}

	private ResultCallback<List<AggregationChunk.NewChunk>> getResultCallback(final int partition) {
		return new ResultCallback<List<AggregationChunk.NewChunk>>() {
			@Override
			public void onResult(List<AggregationChunk.NewChunk> resultChunks) {
				chunks.addAll(resultChunks);
				partitionChunkers.remove(partition);

				if (partitionChunkers.isEmpty() && inputConsumer.getConsumerStatus() == StreamStatus.END_OF_STREAM &&
						!returnedResult) {
					chunksCallback.onResult(chunks);
				}
			}

			@Override
			public void onException(Exception e) {
				logger.error("Chunker for partition #{} failed", partition, e);
				closeWithError(e);
				reportException(e);
			}
		};
	}

	private void reportException(Exception e) {
		if (!returnedResult) {
			chunksCallback.onException(e);
			returnedResult = true;
		}
	}
}
