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
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.async.StagesAccumulator;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public final class AggregationGroupReducer<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final AggregationChunkStorage storage;
	private final AggregationStructure aggregation;
	private final List<String> fields;
	private final PartitionPredicate<T> partitionPredicate;
	private final Class<T> recordClass;
	private final Function<T, Comparable<?>> keyFunction;
	private final Aggregate aggregate;
	private final StagesAccumulator<List<AggregationChunk>> resultsTracker;
	private final DefiningClassLoader classLoader;
	private int chunkSize;

	private final HashMap<Comparable<?>, Object> map = new HashMap<>();

	public AggregationGroupReducer(Eventloop eventloop, AggregationChunkStorage storage,
	                               AggregationStructure aggregation, List<String> fields,
	                               Class<?> recordClass, PartitionPredicate<T> partitionPredicate,
	                               Function<T, Comparable<?>> keyFunction, Aggregate aggregate,
	                               int chunkSize, DefiningClassLoader classLoader) {
		super(eventloop);
		this.storage = storage;
		this.fields = fields;
		this.partitionPredicate = partitionPredicate;
		this.recordClass = (Class<T>) recordClass;
		this.keyFunction = keyFunction;
		this.aggregate = aggregate;
		this.chunkSize = chunkSize;
		this.aggregation = aggregation;
		this.resultsTracker = StagesAccumulator.<List<AggregationChunk>>create(new ArrayList<>())
				.withStage(getCompletionStage(), (accumulator, value) -> accumulator);
		this.classLoader = classLoader;
	}

	public CompletionStage<List<AggregationChunk>> getResult() {
		return resultsTracker.get();
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

	@Override
	protected void onStarted() {
		getProducer().produce(this);
	}

	@SuppressWarnings("unchecked")
	private void doFlush() {
		if (map.isEmpty())
			return;

		suspendOrResume();

		List<Map.Entry<Comparable<?>, Object>> entryList = new ArrayList<>(map.entrySet());
		map.clear();

		entryList.sort((o1, o2) -> {
			Comparable<Object> key1 = (Comparable<Object>) o1.getKey();
			Comparable<Object> key2 = (Comparable<Object>) o2.getKey();
			return key1.compareTo(key2);
		});

		List<Object> list = new ArrayList<>(entryList.size());
		for (Map.Entry<Comparable<?>, Object> entry : entryList) {
			list.add(entry.getValue());
		}

		StreamProducer producer = StreamProducers.ofIterable(eventloop, list);
		AggregationChunker<T> chunker = AggregationChunker.create(eventloop, aggregation, fields, recordClass,
				partitionPredicate, storage, classLoader);
		producer.streamTo(chunker);

		resultsTracker.addStage(chunker.getResult(), (accumulator, newChunks) -> {
			accumulator.addAll(newChunks);
			return accumulator;
		}).thenAccept(aggregationChunks -> suspendOrResume());
	}

	private void suspendOrResume() {
		if (resultsTracker.getActiveStages() > 2) {
			getProducer().suspend();
		} else {
			getProducer().produce(this);
		}
	}

	@Override
	public void onEndOfStream() {
		doFlush();
	}

	@Override
	protected void onError(Exception e) {
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
