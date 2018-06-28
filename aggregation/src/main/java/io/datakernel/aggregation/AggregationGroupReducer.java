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

import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.async.Stage;
import io.datakernel.async.StagesAccumulator;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class AggregationGroupReducer<C, T> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, List<AggregationChunk>>, StreamDataReceiver<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final AggregationChunkStorage<C> storage;
	private final AggregationStructure aggregation;
	private final List<String> measures;
	private final PartitionPredicate<T> partitionPredicate;
	private final Class<T> recordClass;
	private final Function<T, Comparable<?>> keyFunction;
	private final Aggregate aggregate;
	private final StagesAccumulator<List<AggregationChunk>> resultsTracker;
	private final DefiningClassLoader classLoader;
	private final int chunkSize;

	private final HashMap<Comparable<?>, Object> map = new HashMap<>();

	public AggregationGroupReducer(AggregationChunkStorage<C> storage,
			AggregationStructure aggregation, List<String> measures,
			Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
			Function<T, Comparable<?>> keyFunction, Aggregate aggregate,
			int chunkSize, DefiningClassLoader classLoader) {
		this.storage = storage;
		this.measures = measures;
		this.partitionPredicate = partitionPredicate;
		this.recordClass = recordClass;
		this.keyFunction = keyFunction;
		this.aggregate = aggregate;
		this.chunkSize = chunkSize;
		this.aggregation = aggregation;
		this.resultsTracker = StagesAccumulator.<List<AggregationChunk>>create(new ArrayList<>())
				.withStage(this.getEndOfStream(), (accumulator, $) -> {});
		this.classLoader = classLoader;
	}

	@Override
	public Stage<List<AggregationChunk>> getResult() {
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

		List<T> list = new ArrayList<>(entryList.size());
		for (Map.Entry<Comparable<?>, Object> entry : entryList) {
			list.add((T) entry.getValue());
		}

		StreamProducer<T> producer = StreamProducer.ofIterable(list);
		AggregationChunker<C, T> chunker = AggregationChunker.create(aggregation, measures, recordClass,
				partitionPredicate, storage, classLoader, chunkSize);

		resultsTracker.addStage(producer.streamTo(chunker).getConsumerResult(), List::addAll)
				.thenRun(this::suspendOrResume);
	}

	private void suspendOrResume() {
		if (resultsTracker.getActiveStages() > 2) {
			logger.trace("Suspend group reduce: {}", this);
			getProducer().suspend();
		} else {
			logger.trace("Resume group reduce: {}", this);
			getProducer().produce(this);
		}
	}

	@Override
	public void onEndOfStream() {
		doFlush();
	}

	@Override
	protected void onError(Throwable t) {
	}

	// jmx
	public void flush() {
		doFlush();
	}

	public int getBufferSize() {
		return map.size();
	}

	@Override
	public String toString() {
		return "AggregationGroupReducer{" +
				"keys=" + aggregation.getKeys() +
				"measures=" + measures +
				", chunkSize=" + chunkSize +
				", map.size=" + map.size() +
				'}';
	}
}
