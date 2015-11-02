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
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerDecorator;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class AggregationChunker<T> extends StreamConsumerDecorator<T> {
	private static final Logger logger = LoggerFactory.getLogger(AggregationChunker.class);

	public AggregationChunker(Eventloop eventloop, String aggregationId, List<String> keys, List<String> fields,
	                          Class<T> recordClass, AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
	                          int chunkSize, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		ChunkerTransformer chunkerTransformer = new ChunkerTransformer(eventloop, aggregationId, keys, fields, recordClass,
				storage, metadataStorage, chunkSize, chunksCallback);
		setActualConsumer(chunkerTransformer.getInput());
	}

	private class ChunkerTransformer extends AbstractStreamTransformer_1_1<T, T> {
		private InputConsumer inputConsumer;
		private OutputProducer outputProducer;

		protected ChunkerTransformer(Eventloop eventloop, String aggregationId, List<String> keys, List<String> fields,
		                             Class<T> recordClass, AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
		                             int chunkSize, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
			super(eventloop);
			this.outputProducer = new OutputProducer();
			this.inputConsumer = new InputConsumer(aggregationId, keys, fields, recordClass,
					storage, metadataStorage, chunkSize, chunksCallback);
		}

		private class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<T> {
			private long newId;
			private final String aggregationId;
			private final List<String> keys;
			private final List<String> fields;
			private final Class<T> recordClass;
			private final ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback;
			private final int chunkSize;

			private T first;
			private T last;
			private int count;

			private int pendingChunks;
			private final List<AggregationChunk.NewChunk> chunks = new ArrayList<>();
			private AggregationChunkStorage storage;
			private AggregationMetadataStorage metadataStorage;

			public InputConsumer(String aggregationId, List<String> keys, List<String> fields,
			                     Class<T> recordClass, AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
			                     int chunkSize, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
				this.aggregationId = aggregationId;
				this.keys = keys;
				this.fields = fields;
				this.recordClass = recordClass;
				this.chunksCallback = chunksCallback;
				this.storage = storage;
				this.metadataStorage = metadataStorage;
				this.chunkSize = chunkSize;
				this.pendingChunks = 1;
				startNewChunk();
			}

			@Override
			protected void onUpstreamEndOfStream() {
				saveChunk();
				outputProducer.sendEndOfStream();
				logger.trace("{}: downstream producer {} closed.", this, outputProducer);
			}

			@Override
			protected void onError(Exception e) {
				super.onError(e);
				chunksCallback.onException(e);
				logger.error("{}: downstream producer {} exception.", this, outputProducer, e);
			}

			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(T item) {
				if (first == null) {
					first = item;
				}
				last = item;

				outputProducer.send(item);

				if (count++ == chunkSize) {
					rotateChunk();
				}
			}

			private void rotateChunk() {
				saveChunk();
				++pendingChunks;
				outputProducer.getDownstream().onProducerEndOfStream();
				startNewChunk();
			}

			private void saveChunk() {
				if (count != 0) {
					AggregationChunk.NewChunk chunk = new AggregationChunk.NewChunk(
							InputConsumer.this.newId,
							fields,
							PrimaryKey.ofObject(first, keys),
							PrimaryKey.ofObject(last, keys),
							count);
					chunks.add(chunk);
				}
			}

			public void startNewChunk() {
				newId = metadataStorage.newChunkId(); // TODO (dtkachenko): refactor as async
				first = null;
				last = null;
				count = 0;

				StreamConsumer<T> consumer = storage.chunkWriter(aggregationId, keys, fields, recordClass, newId, new CompletionCallback() {
					@Override
					public void onComplete() {
						if (--pendingChunks == 0) {
							chunksCallback.onResult(chunks);
						}
						logger.trace("{}: saving new chunk with id {} to storage {} completed.", this, newId, storage);
					}

					@Override
					public void onException(Exception exception) {
						logger.error("{}: saving new chunk with id {} to storage {} failed.", this, newId, storage);
						closeWithError(exception);
					}
				});

				outputProducer.streamTo(consumer);
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
}
