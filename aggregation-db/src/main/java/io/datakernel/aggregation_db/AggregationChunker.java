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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;

public final class AggregationChunker<T> extends StreamConsumerDecorator<T> {
	private static final Logger logger = LoggerFactory.getLogger(AggregationChunker.class);

	public AggregationChunker(Eventloop eventloop, String aggregationId, List<String> keys, List<String> fields,
	                          Class<T> recordClass, AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
	                          int chunkSize, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		Chunker chunker = new Chunker(eventloop, aggregationId, keys, fields, recordClass, storage, metadataStorage,
				chunkSize, chunksCallback);
		setActualConsumer(chunker.getInput());
	}

	private class Chunker extends AbstractStreamTransformer_1_1<T, T> {
		private InputConsumer inputConsumer;
		private OutputProducer outputProducer;

		protected Chunker(Eventloop eventloop, String aggregationId, List<String> keys, List<String> fields,
		                  Class<T> recordClass, AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
		                  int chunkSize, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
			super(eventloop);
			this.outputProducer = new OutputProducer();
			this.inputConsumer = new InputConsumer(aggregationId, keys, fields, recordClass,
					storage, metadataStorage, chunkSize, chunksCallback);
		}

		private class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<T> {
			private final String aggregationId;
			private final List<String> keys;
			private final List<String> fields;
			private final Class<T> recordClass;
			private AggregationChunkStorage storage;
			private AggregationMetadataStorage metadataStorage;
			private final int chunkSize;
			private final ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback;

			private int count;

			private int pendingChunks;
			private boolean returnedResult;

			private final List<AggregationChunk.NewChunk> chunks = new ArrayList<>();

			public InputConsumer(String aggregationId, List<String> keys, List<String> fields,
			                     Class<T> recordClass, AggregationChunkStorage storage,
			                     AggregationMetadataStorage metadataStorage,
			                     int chunkSize, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
				checkArgument(chunkSize > 0);
				this.aggregationId = aggregationId;
				this.keys = keys;
				this.fields = fields;
				this.recordClass = recordClass;
				this.chunksCallback = chunksCallback;
				this.storage = storage;
				this.metadataStorage = metadataStorage;
				this.chunkSize = chunkSize;
			}

			@Override
			protected void onUpstreamEndOfStream() {
				if (outputProducer.getDownstream() != null)
					outputProducer.sendEndOfStream();

				if (pendingChunks == 0 && !returnedResult)
					chunksCallback.onResult(chunks);
			}

			@Override
			protected void onError(Exception e) {
				super.onError(e);
				reportException(e);
			}

			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(T item) {
				if (outputProducer.getDownstream() == null) {
					++pendingChunks;
					startNewChunk();
				} else if (count == chunkSize)
					rotateChunk();

				++count;
				outputProducer.send(item);
			}

			private void rotateChunk() {
				++pendingChunks;
				outputProducer.getDownstream().onProducerEndOfStream();
				startNewChunk();
			}

			private AggregationChunk.NewChunk createNewChunk(long id, T first, T last, int count) {
				return new AggregationChunk.NewChunk(id, fields, PrimaryKey.ofObject(first, keys),
						PrimaryKey.ofObject(last, keys), count);
			}

			private void startNewChunk() {
				count = 0;
				final MetadataCounter metadataCounter = new MetadataCounter(eventloop);
				outputProducer.streamTo(metadataCounter.getInput());

				metadataStorage.newChunkId(new ResultCallback<Long>() {
					@Override
					public void onResult(final Long chunkId) {
						storage.chunkWriter(aggregationId, keys, fields, recordClass, chunkId, metadataCounter.getOutput(),
								new CompletionCallback() {
									@Override
									public void onComplete() {
										AggregationChunk.NewChunk newChunk = createNewChunk(chunkId,
												metadataCounter.first, metadataCounter.last, metadataCounter.count);
										chunks.add(newChunk);

										if (--pendingChunks == 0 && getConsumerStatus() == StreamStatus.END_OF_STREAM && !returnedResult) {
											chunksCallback.onResult(chunks);
											returnedResult = true;
										}

										logger.trace("Saving new chunk with id {} to storage {} completed",
												chunkId, storage);
									}

									@Override
									public void onException(Exception e) {
										logger.error("Saving new chunk with id {} to storage {} failed",
												chunkId, storage, e);
										--pendingChunks;
										closeWithError(e);
										reportException(e);
									}
								});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Failed to retrieve new chunk id from metadata storage {}",
								metadataStorage, e);
						--pendingChunks;
						closeWithError(e);
						reportException(e);
					}
				});
			}

			private void reportException(Exception e) {
				if (!returnedResult) {
					chunksCallback.onException(e);
					returnedResult = true;
				}
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
	}

	private final class MetadataCounter extends AbstractStreamTransformer_1_1<T, T> {
		private final InputConsumer inputConsumer;
		private final OutputProducer outputProducer;

		private T first;
		private T last;
		private int count;

		private final class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<T> {
			@Override
			public void onData(T item) {
				if (first == null)
					first = item;

				last = item;
				++count;
				outputProducer.send(item);
			}

			@Override
			protected void onUpstreamEndOfStream() {
				outputProducer.sendEndOfStream();
			}

			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return this;
			}
		}

		private final class OutputProducer extends AbstractOutputProducer {
			@Override
			protected void onDownstreamSuspended() {
				inputConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				inputConsumer.resume();
			}
		}

		private MetadataCounter(Eventloop eventloop) {
			super(eventloop);
			this.inputConsumer = new InputConsumer();
			this.outputProducer = new OutputProducer();
		}
	}
}
