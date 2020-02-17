/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.async.process.AsyncCollector;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.datastream.AbstractStreamConsumer;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class AggregationGroupReducer<C, T, K extends Comparable> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
	private static final Logger logger = LoggerFactory.getLogger(AggregationGroupReducer.class);

	private final AggregationChunkStorage<C> storage;
	private final AggregationStructure aggregation;
	private final List<String> measures;
	private final PartitionPredicate<T> partitionPredicate;
	private final Class<T> recordClass;
	private final Function<T, K> keyFunction;
	private final Aggregate<T, Object> aggregate;
	private final AsyncCollector<List<AggregationChunk>> chunksCollector;
	private final DefiningClassLoader classLoader;
	private final int chunkSize;

	private final HashMap<K, Object> map = new HashMap<>();

	public AggregationGroupReducer(@NotNull AggregationChunkStorage<C> storage,
			@NotNull AggregationStructure aggregation, @NotNull List<String> measures,
			@NotNull Class<T> recordClass, @NotNull PartitionPredicate<T> partitionPredicate,
			@NotNull Function<T, K> keyFunction, @NotNull Aggregate<T, Object> aggregate,
			int chunkSize, @NotNull DefiningClassLoader classLoader) {
		this.storage = storage;
		this.measures = measures;
		this.partitionPredicate = partitionPredicate;
		this.recordClass = recordClass;
		this.keyFunction = keyFunction;
		this.aggregate = aggregate;
		this.chunkSize = chunkSize;
		this.aggregation = aggregation;
		this.chunksCollector = AsyncCollector.create(new ArrayList<>());
		this.classLoader = classLoader;
	}

	public Promise<List<AggregationChunk>> getResult() {
		return chunksCollector.get();
	}

	@Override
	public void accept(T item) {
		K key = keyFunction.apply(item);
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
		resume(this);
	}

	@SuppressWarnings("unchecked")
	private void doFlush() {
		if (map.isEmpty())
			return;

		suspendOrResume();

		List<Map.Entry<K, Object>> entryList = new ArrayList<>(map.entrySet());
		map.clear();

		entryList.sort((o1, o2) -> {
			K key1 = o1.getKey();
			K key2 = o2.getKey();
			return key1.compareTo(key2);
		});

		List<T> list = new ArrayList<>(entryList.size());
		for (Map.Entry<K, Object> entry : entryList) {
			list.add((T) entry.getValue());
		}

		StreamSupplier<T> supplier = StreamSupplier.ofIterable(list);
		AggregationChunker<C, T> chunker = AggregationChunker.create(aggregation, measures, recordClass,
				partitionPredicate, storage, classLoader, chunkSize);

		chunksCollector.addPromise(
				supplier.streamTo(chunker)
						.then(chunker::getResult),
				List::addAll)
				.whenResult(this::suspendOrResume);
	}

	private void suspendOrResume() {
		if (chunksCollector.getActivePromises() > 2) {
			logger.trace("Suspend group reduce: {}", this);
			suspend();
		} else {
			logger.trace("Resume group reduce: {}", this);
			resume(this);
		}
	}

	@Override
	protected void onEndOfStream() {
		doFlush();
		chunksCollector.run().get().toVoid()
				.whenResult(this::acknowledge)
				.whenException(this::closeEx);
	}

	@Override
	protected void onError(Throwable e) {
		chunksCollector.closeEx(e);
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
