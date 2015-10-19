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

import com.google.common.base.Function;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.collect.Iterables.transform;

public final class AggregationGroupReducer<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private static final Logger logger = LoggerFactory.getLogger(AggregationGroupReducer.class);

	private final AggregationChunkStorage storage;
	private final AggregationMetadataStorage metadataStorage;
	private final AggregationMetadata aggregationMetadata;
	private final List<String> keys;
	private final List<String> outputFields;
	private final Class<?> accumulatorClass;
	private final Function<T, Comparable<?>> keyFunction;
	private final Aggregate aggregate;
	private final ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback;
	private final int chunkSize;

	private final HashMap<Comparable<?>, Object> map = new HashMap<>();

	private final List<AggregationChunk.NewChunk> chunks = new ArrayList<>();
	private boolean saving;

	public AggregationGroupReducer(Eventloop eventloop, AggregationChunkStorage storage, AggregationMetadataStorage metadataStorage,
	                               AggregationMetadata aggregationMetadata, List<String> fields,
	                               Class<?> accumulatorClass,
	                               Function<T, Comparable<?>> keyFunction, Aggregate aggregate,
	                               ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback, int chunkSize) {
		super(eventloop);
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.aggregationMetadata = aggregationMetadata;
		this.keys = aggregationMetadata.getKeys();
		this.outputFields = aggregationMetadata.getOutputFields();
		this.accumulatorClass = accumulatorClass;
		this.keyFunction = keyFunction;
		this.aggregate = aggregate;
		this.chunksCallback = chunksCallback;
		this.chunkSize = chunkSize;
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return this;
	}

	@Override
	public void onData(T item) {
		Comparable<?> key = keyFunction.apply(item);
		Object accumulator = map.get(key);
		if (accumulator != null) {
			aggregate.accumulate(accumulator, item);
		} else {
			accumulator = aggregate.createAccumulator(item);
			map.put(key, accumulator);

			if (map.size() == chunkSize) {
				doNext();
			}
		}
	}

	private void doNext() {
		if (saving) {
			suspend();
			return;
		}

		if (getConsumerStatus() == StreamStatus.END_OF_STREAM && map.isEmpty()) {
			chunksCallback.onResult(chunks);
			logger.trace("{}: completed saving chunks {} for aggregation {}. Closing itself.", this, chunks, aggregationMetadata);
			return;
		}

		if (map.isEmpty()) {
			return;
		}

		saving = true;

		final List<Map.Entry<Comparable<?>, Object>> entryList = new ArrayList<>(map.entrySet());
		map.clear();

		Collections.sort(entryList, new Comparator<Map.Entry<Comparable<?>, Object>>() {
			@SuppressWarnings("unchecked")
			@Override
			public int compare(Map.Entry<Comparable<?>, Object> o1, Map.Entry<Comparable<?>, Object> o2) {
				Comparable<Object> key1 = (Comparable<Object>) o1.getKey();
				Comparable<Object> key2 = (Comparable<Object>) o2.getKey();
				return key1.compareTo(key2);
			}
		});

		metadataStorage.newChunkId(new ResultCallback<Long>() {
			@SuppressWarnings("unchecked")
			@Override
			public void onResult(Long newId) {
				AggregationChunk.NewChunk newChunk = new AggregationChunk.NewChunk(
						newId,
						outputFields,
						PrimaryKey.ofObject(entryList.get(0).getValue(), keys),
						PrimaryKey.ofObject(entryList.get(entryList.size() - 1).getValue(), keys),
						entryList.size());
				chunks.add(newChunk);

				Iterable<Object> list = transform(entryList, new Function<Map.Entry<Comparable<?>, Object>, Object>() {
					@Override
					public Object apply(Map.Entry<Comparable<?>, Object> input) {
						return input.getValue();
					}
				});

				final StreamProducer<Object> producer = StreamProducers.ofIterable(eventloop, list);

				StreamConsumer consumer = storage.chunkWriter(aggregationMetadata.getId(), keys, outputFields, accumulatorClass, newId, new CompletionCallback() {
					@Override
					public void onComplete() {
						saving = false;
						eventloop.post(new Runnable() {
							@Override
							public void run() {
								doNext();
							}
						});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Saving chunks {} to aggregation storage {} failed.", chunks, storage, e);
					}
				});

				producer.streamTo(consumer);

				AggregationGroupReducer.this.resume();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Failed to retrieve new chunk id from the metadata storage {}.", metadataStorage);
				// TODO (dtkachenko): implement proper exception handling after merge with async-streams2
			}
		});
	}

	@Override
	public void onEndOfStream() {
		logger.trace("{}: upstream producer {} closed.", this, upstreamProducer);
		doNext();
	}
}
