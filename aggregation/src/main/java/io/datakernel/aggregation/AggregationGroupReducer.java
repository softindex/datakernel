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

import com.google.common.base.Function;
import io.datakernel.aggregation.util.AsyncResultsReducer;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.datakernel.async.AsyncCallbacks.postTo;

final class AggregationGroupReducer<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final AggregationChunkStorage storage;
	private final AggregationMetadataStorage metadataStorage;
	private final Aggregation aggregation;
	private final List<String> keys;
	private final List<String> fields;
	private final PartitionPredicate<T> partitionPredicate;
	private final Class<?> recordClass;
	private final Function<T, Comparable<?>> keyFunction;
	private final Aggregate aggregate;
	private final AsyncResultsReducer<List<AggregationChunk.NewChunk>> resultsTracker;
	private final DefiningClassLoader classLoader;
	private int chunkSize;

	private final HashMap<Comparable<?>, Object> map = new HashMap<>();

	private static final AsyncResultsReducer.ResultReducer<List<AggregationChunk.NewChunk>, List<AggregationChunk.NewChunk>> REDUCER = new AsyncResultsReducer.ResultReducer<List<AggregationChunk.NewChunk>, List<AggregationChunk.NewChunk>>() {
		@Override
		public List<AggregationChunk.NewChunk> applyResult(List<AggregationChunk.NewChunk> accumulator, List<AggregationChunk.NewChunk> value) {
			accumulator.addAll(value);
			return accumulator;
		}
	};

	public AggregationGroupReducer(Eventloop eventloop, AggregationChunkStorage storage,
	                               AggregationMetadataStorage metadataStorage,
	                               Aggregation aggregation, List<String> keys, List<String> fields,
	                               Class<?> recordClass, PartitionPredicate<T> partitionPredicate,
	                               Function<T, Comparable<?>> keyFunction, Aggregate aggregate,
	                               int chunkSize, DefiningClassLoader classLoader,
	                               final ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		super(eventloop);
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.keys = keys;
		this.fields = fields;
		this.partitionPredicate = partitionPredicate;
		this.recordClass = recordClass;
		this.keyFunction = keyFunction;
		this.aggregate = aggregate;
		this.chunkSize = chunkSize;
		this.aggregation = aggregation;
		this.resultsTracker = AsyncResultsReducer.create(postTo(chunksCallback), new ArrayList<AggregationChunk.NewChunk>());
		this.classLoader = classLoader;
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

		List<Object> list = new ArrayList<>(entryList.size());
		for (Map.Entry<Comparable<?>, Object> entry : entryList) {
			list.add(entry.getValue());
		}

		final ResultCallback<List<AggregationChunk.NewChunk>> callback = resultsTracker.newResultCallback(REDUCER);

		StreamProducer producer = StreamProducers.ofIterable(eventloop, list);
		AggregationChunker consumer = new AggregationChunker(eventloop, aggregation, keys, fields, recordClass,
				partitionPredicate, storage, metadataStorage, chunkSize, classLoader,
				new ResultCallback<List<AggregationChunk.NewChunk>>() {
					@Override
					protected void onResult(List<AggregationChunk.NewChunk> newChunks) {
						callback.setResult(newChunks);
						resume();
					}

					@Override
					protected void onException(Exception e) {
						logger.error("Streaming to chunker failed", e);
						callback.setException(e);
						closeWithError(e);
					}
				});
		producer.streamTo(consumer);
	}

	@Override
	public void onEndOfStream() {
		doFlush();
		resultsTracker.setComplete();
	}

	@Override
	protected void onError(Exception e) {
		resultsTracker.setException(e);
	}

	// jmx
	public void flush() {
		doFlush();
	}

	public int getBufferSize() {
		return map.size();
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}
}
