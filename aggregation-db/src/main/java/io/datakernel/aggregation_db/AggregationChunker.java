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

public final class AggregationChunker<T> extends StreamConsumerDecorator<T> implements StreamDataReceiver<T> {
	private static final Logger logger = LoggerFactory.getLogger(AggregationChunker.class);

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

	private StreamConsumerSwitcher<T> switcher = new StreamConsumerSwitcher<>(eventloop);

	public AggregationChunker(Eventloop eventloop, String aggregationId, List<String> keys, List<String> fields,
	                          Class<T> recordClass, AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
	                          int chunkSize, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		super(eventloop);
		this.aggregationId = aggregationId;
		this.keys = keys;
		this.fields = fields;
		this.recordClass = recordClass;
		this.chunksCallback = chunksCallback;
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.chunkSize = chunkSize;
		setActualConsumer(switcher);
		startNewChunk();
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
		switcher.getDataReceiver().onData(item);

		if (count++ == chunkSize) {
			rotateChunk();
		}
	}

	private void rotateChunk() {
		saveChunk();
		StreamConsumer<T> chunkConsumer = switcher.getCurrentConsumer();
		new StreamProducers.EndOfStream<T>(eventloop).streamTo(chunkConsumer);
		startNewChunk();
	}

	private void saveChunk() {
		if (count != 0) {
			AggregationChunk.NewChunk chunk = new AggregationChunk.NewChunk(
					this.newId,
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
		pendingChunks++;

		StreamConsumer<T> consumer = storage.chunkWriter(aggregationId, keys, fields, recordClass, newId);

		switcher.switchConsumerTo(consumer);

		consumer.addCompletionCallback(new CompletionCallback() {
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
			}
		});
	}

	@Override
	public void onProducerEndOfStream() {
		super.onProducerEndOfStream();
		saveChunk();
		logger.trace("{}: upstream producer {} closed.", this, upstreamProducer);
	}

	@Override
	public void onProducerError(Exception e) {
		// TODO (dvolvach)
		logger.error("{}: upstream producer {} exception.", this, upstreamProducer, e);
	}
}
