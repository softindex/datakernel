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
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executor;

import static com.google.common.collect.Iterables.transform;

public final class AggregationGroupReducer<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T>, ConcurrentJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(AggregationGroupReducer.class);

	private final AggregationChunkStorage storage;
	private final AggregationMetadataStorage metadataStorage;
	private final String aggregationId;
	private final List<String> keys;
	private final List<String> fields;
	private final PartitioningStrategy partitioningStrategy;
	private final Class<?> recordClass;
	private final Function<T, Comparable<?>> keyFunction;
	private final Aggregate aggregate;
	private final ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback;
	private final int chunkSize;

	private final HashMap<Comparable<?>, Object> map = new HashMap<>();

	private final List<AggregationChunk.NewChunk> chunks = new ArrayList<>();

	private int pendingWriters;
	private boolean returnedResult;

	public AggregationGroupReducer(Eventloop eventloop, AggregationChunkStorage storage,
	                               AggregationMetadataStorage metadataStorage, AggregationMetadata aggregationMetadata,
	                               PartitioningStrategy partitioningStrategy, Class<?> recordClass,
	                               Function<T, Comparable<?>> keyFunction, Aggregate aggregate,
	                               ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback, int chunkSize) {
		super(eventloop);
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.aggregationId = aggregationMetadata.getId();
		this.keys = aggregationMetadata.getKeys();
		this.fields = aggregationMetadata.getOutputFields();
		this.partitioningStrategy = partitioningStrategy;
		this.recordClass = recordClass;
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
				doFlush();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void doFlush() {
		if (map.isEmpty())
			return;

		++pendingWriters;

		if (pendingWriters > 1)
			suspend();

		final List<Map.Entry<Comparable<?>, Object>> entryList = new ArrayList<>(map.entrySet());
		map.clear();

		Collections.sort(entryList, new Comparator<Map.Entry<Comparable<?>, Object>>() {
			@Override
			public int compare(Map.Entry<Comparable<?>, Object> o1, Map.Entry<Comparable<?>, Object> o2) {
				Comparable<Object> key1 = (Comparable<Object>) o1.getKey();
				Comparable<Object> key2 = (Comparable<Object>) o2.getKey();
				return key1.compareTo(key2);
			}
		});

		Iterable<Object> list = transform(entryList, new Function<Map.Entry<Comparable<?>, Object>, Object>() {
			@Override
			public Object apply(Map.Entry<Comparable<?>, Object> input) {
				return input.getValue();
			}
		});

		final StreamProducer producer = StreamProducers.ofIterable(eventloop, list);

		producer.streamTo(Aggregation.createChunker(partitioningStrategy, eventloop, aggregationId, keys, fields,
				recordClass, storage, metadataStorage, chunkSize,
				new ResultCallback<List<AggregationChunk.NewChunk>>() {
					@Override
					public void onResult(List<AggregationChunk.NewChunk> newChunks) {
						chunks.addAll(newChunks);

						if (--pendingWriters == 0 && getConsumerStatus() == StreamStatus.END_OF_STREAM && !returnedResult) {
							chunksCallback.onResult(chunks);
							returnedResult = true;
						}

						if (pendingWriters < 2)
							resume();
					}

					@Override
					public void onException(Exception e) {
						logger.error("Saving chunks to aggregation storage {} failed", storage, e);
						--pendingWriters;
						closeWithError(e);
						reportException(e);
					}
				}));
	}

	@Override
	public void onEndOfStream() {
		doFlush();

		if (pendingWriters == 0 && !returnedResult)
			chunksCallback.onResult(chunks);
	}

	@Override
	protected void onError(Exception e) {
		reportException(e);
	}

	private void reportException(Exception e) {
		if (!returnedResult) {
			chunksCallback.onException(e);
			returnedResult = true;
		}
	}

	// jmx

	@JmxOperation
	public void flush() {
		doFlush();
	}

	@Override
	public Executor getJmxExecutor() {
		return eventloop;
	}
}
