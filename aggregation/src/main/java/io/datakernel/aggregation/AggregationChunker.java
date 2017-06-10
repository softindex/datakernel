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

import io.datakernel.aggregation.util.AsyncResultsReducer;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.AsyncCallbacks.postTo;

final class AggregationChunker<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Aggregation aggregation;
	private final List<String> keys;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final PartitionPredicate<T> partitionPredicate;
	private final AggregationChunkStorage storage;
	private final AggregationMetadataStorage metadataStorage;
	private final AsyncResultsReducer<List<AggregationChunk>> chunksAccumulator;
	private final DefiningClassLoader classLoader;

	private StreamDataReceiver<T> downstreamDataReceiver;

	private AbstractStreamProducer<T> outputProducer = new AbstractStreamProducer<T>(eventloop) {
		@Override
		protected final void onDataReceiverChanged() {
			AggregationChunker.this.downstreamDataReceiver = this.downstreamDataReceiver;
		}

		@Override
		protected void onSuspended() {
			applySuspendOrResume();
		}

		@Override
		protected void onResumed() {
			applySuspendOrResume();
		}

		@Override
		protected void onError(Exception e) {
			AggregationChunker.this.closeWithError(e);
		}
	};

	private static final AsyncResultsReducer.ResultReducer<List<AggregationChunk>, AggregationChunk> REDUCER = new AsyncResultsReducer.ResultReducer<List<AggregationChunk>, AggregationChunk>() {
		@Override
		public List<AggregationChunk> applyResult(List<AggregationChunk> accumulator, AggregationChunk value) {
			accumulator.add(value);
			return accumulator;
		}
	};

	public AggregationChunker(Eventloop eventloop,
	                          Aggregation aggregation, List<String> keys, List<String> fields,
	                          Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
	                          AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
	                          int chunkSize, DefiningClassLoader classLoader,
	                          final ResultCallback<List<AggregationChunk>> chunksCallback) {
		super(eventloop);
		this.aggregation = aggregation;
		this.keys = keys;
		this.fields = fields;
		this.recordClass = recordClass;
		this.partitionPredicate = partitionPredicate;
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.chunksAccumulator = AsyncResultsReducer.create(postTo(chunksCallback), new ArrayList<AggregationChunk>());
		this.chunkSize = chunkSize;
		this.classLoader = classLoader;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	private int chunkSize;

	private T first;
	private T last;
	private int count = Integer.MAX_VALUE;
	private Metadata<T> currentChunkMetadata;

	private static final class Metadata<T> {
		private T first;
		private T last;
		private int count;

		private void set(T first, T last, int count) {
			this.first = first;
			this.last = last;
			this.count = count;
		}
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return this;
	}

	@Override
	protected void onError(Exception e) {
		chunksAccumulator.setException(e);
		outputProducer.closeWithError(e);
	}

	@Override
	public void onData(T item) {
		if (count >= chunkSize || !partitionPredicate.isSamePartition(last, item)) {
			rotateChunk();
			first = item;
		}

		++count;

		downstreamDataReceiver.onData(item);
		last = item;
	}

	@Override
	protected void onEndOfStream() {
		if (outputProducer.getDownstream() != null) {
			currentChunkMetadata.set(first, last, count);
			StreamProducers.<T>closing(eventloop).streamTo(outputProducer.getDownstream());
		}
		chunksAccumulator.setComplete();
	}

	private void rotateChunk() {
		if (outputProducer.getDownstream() != null) {
			currentChunkMetadata.set(first, last, count);
			StreamProducers.<T>closing(eventloop).streamTo(outputProducer.getDownstream());
		}
		startNewChunk();
	}

	private void startNewChunk() {
		first = null;
		last = null;
		count = 0;

		final Metadata<T> metadata = new Metadata<>();
		currentChunkMetadata = metadata;

		final StreamForwarder<T> forwarder = StreamForwarder.create(eventloop);
		outputProducer.streamTo(forwarder.getInput());

		logger.info("Retrieving new chunk id for aggregation {}", keys);

		final ResultCallback<AggregationChunk> newChunkCallback = chunksAccumulator.newResultCallback(REDUCER);
		applySuspendOrResume();

		metadataStorage.createChunkId(new ResultCallback<Long>() {
			@Override
			protected void onResult(final Long chunkId) {
				logger.info("Retrieved new chunk id '{}' for aggregation {}", chunkId, keys);
				storage.write(forwarder.getOutput(), aggregation, keys, fields, recordClass, chunkId, classLoader, new CompletionCallback() {
					@Override
					protected void onComplete() {
						AggregationChunk newChunk = AggregationChunk.create(chunkId,
								fields,
								PrimaryKey.ofObject(metadata.first, keys),
								PrimaryKey.ofObject(metadata.last, keys),
								metadata.count);
						newChunkCallback.setResult(newChunk);
						applySuspendOrResume();
						logger.trace("Saving new chunk with id {} to storage {} completed", chunkId, storage);
					}

					@Override
					protected void onException(Exception e) {
						logger.error("Saving new chunk with id {} to storage {} failed", chunkId, storage, e);
						closeWithError(e);
						newChunkCallback.setException(e);
					}
				});
			}

			@Override
			protected void onException(Exception e) {
				logger.error("Failed to retrieve new chunk id from metadata storage {}", metadataStorage, e);
				closeWithError(e);
				newChunkCallback.setException(e);
			}
		});
	}

	private void applySuspendOrResume() {
		if (outputProducer.getProducerStatus() == StreamStatus.READY && chunksAccumulator.getActiveCallbacks() <= 1) {
			resume();
		} else {
			suspend();
		}
	}
}