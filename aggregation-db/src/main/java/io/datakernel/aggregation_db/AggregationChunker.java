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

import io.datakernel.aggregation_db.util.AsyncResultsTracker;
import io.datakernel.aggregation_db.util.AsyncResultsTracker.AsyncResultsTrackerList;
import io.datakernel.aggregation_db.util.BiPredicate;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamConsumerDecorator;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamForwarder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;

public final class AggregationChunker<T> extends StreamConsumerDecorator<T> {
	private static final Logger logger = LoggerFactory.getLogger(AggregationChunker.class);

	private final List<String> keys;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final BiPredicate<T, T> partitionPredicate;
	private final AggregationChunkStorage storage;
	private final AggregationMetadataStorage metadataStorage;
	private final Chunker chunker;
	private final AsyncResultsTrackerList<AggregationChunk.NewChunk> resultsTracker;
	private final DefiningClassLoader classLoader;

	public AggregationChunker(Eventloop eventloop, final AggregationOperationTracker operationTracker,
	                          List<String> keys, List<String> fields,
	                          Class<T> recordClass, BiPredicate<T, T> partitionPredicate,
	                          AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
	                          int chunkSize, DefiningClassLoader classLoader,
	                          final ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		this.keys = keys;
		this.fields = fields;
		this.recordClass = recordClass;
		this.partitionPredicate = partitionPredicate;
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.resultsTracker = AsyncResultsTracker.ofList(new ResultCallback<List<AggregationChunk.NewChunk>>() {
			@Override
			public void onResult(List<AggregationChunk.NewChunk> result) {
				operationTracker.reportCompletion(AggregationChunker.this);
				chunksCallback.onResult(result);
			}

			@Override
			public void onException(Exception e) {
				operationTracker.reportCompletion(AggregationChunker.this);
				chunksCallback.onException(e);
			}
		});
		this.classLoader = classLoader;
		this.chunker = new Chunker(eventloop, chunkSize);
		setActualConsumer(chunker.getInput());
		operationTracker.reportStart(this);
	}

	public void setChunkSize(int chunkSize) {
		this.chunker.inputConsumer.chunkSize = chunkSize;
	}

	private class Chunker extends AbstractStreamTransformer_1_1<T, T> {
		private final InputConsumer inputConsumer;
		private final OutputProducer outputProducer;

		protected Chunker(Eventloop eventloop, int chunkSize) {
			super(eventloop);
			this.outputProducer = new OutputProducer();
			this.inputConsumer = new InputConsumer(chunkSize);
		}

		private class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<T> {
			private int chunkSize;

			private T first;
			private T last;
			private int count = Integer.MAX_VALUE;
			private Metadata currentChunkMetadata;

			public InputConsumer(int chunkSize) {
				checkArgument(chunkSize > 0);
				this.chunkSize = chunkSize;
			}

			@Override
			protected void onUpstreamEndOfStream() {
				if (outputProducer.getDownstream() != null) {
					currentChunkMetadata.init(first, last, count);
					outputProducer.sendEndOfStream();
				}

				resultsTracker.shutDown();
			}

			@Override
			protected void onError(Exception e) {
				super.onError(e);
				resultsTracker.shutDownWithException(e);
			}

			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(T item) {
				if (count >= chunkSize || !partitionPredicate.test(last, item)) {
					rotateChunk();
					first = item;
				}

				++count;

				downstreamDataReceiver.onData(item);
				last = item;
			}

			private void rotateChunk() {
				if (outputProducer.getDownstream() != null) {
					currentChunkMetadata.init(first, last, count);
					outputProducer.getDownstream().onProducerEndOfStream();
				}
				startNewChunk();
			}

			private AggregationChunk.NewChunk createNewChunk(long id, T first, T last, int count) {
				return new AggregationChunk.NewChunk(id, fields, PrimaryKey.ofObject(first, keys),
						PrimaryKey.ofObject(last, keys), count);
			}

			private void startNewChunk() {
				resultsTracker.startOperation();
				first = null;
				last = null;
				count = 0;

				final Metadata metadata = new Metadata();
				currentChunkMetadata = metadata;

				final StreamForwarder<T> forwarder = new StreamForwarder<>(eventloop);
				outputProducer.streamTo(forwarder.getInput());

				logger.info("Retrieving new chunk id for aggregation {}", keys);
				metadataStorage.createChunkId(new ResultCallback<Long>() {
					@Override
					public void onResult(final Long chunkId) {
						logger.info("Retrieved new chunk id '{}' for aggregation {}", chunkId, keys);
						storage.chunkWriter(keys, fields, recordClass, chunkId, forwarder.getOutput(), classLoader,
								new CompletionCallback() {
									@Override
									public void onComplete() {
										AggregationChunk.NewChunk newChunk = createNewChunk(chunkId, metadata.first,
												metadata.last, metadata.count);

										resultsTracker.completeWithResult(newChunk);

										logger.trace("Saving new chunk with id {} to storage {} completed",
												chunkId, storage);
									}

									@Override
									public void onException(Exception e) {
										logger.error("Saving new chunk with id {} to storage {} failed",
												chunkId, storage, e);
										closeWithError(e);
										resultsTracker.completeWithException(e);
									}
								});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Failed to retrieve new chunk id from metadata storage {}",
								metadataStorage, e);
						closeWithError(e);
						resultsTracker.completeWithException(e);
					}
				});
			}
		}

		private class OutputProducer extends AbstractOutputProducer {
			@Override
			protected void onDownstreamSuspended() {
				inputConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				inputConsumer.resume();
			}
		}

		private final class Metadata {
			private T first;
			private T last;
			private int count;

			private void init(T first, T last, int count) {
				this.first = first;
				this.last = last;
				this.count = count;
			}
		}
	}
}
