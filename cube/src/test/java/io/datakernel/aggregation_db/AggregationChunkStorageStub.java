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
import com.google.common.collect.Iterables;
import io.datakernel.async.CompletionCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.codegen.Expressions.*;

@SuppressWarnings("unchecked")
public class AggregationChunkStorageStub implements AggregationChunkStorage {
	private final Eventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final Map<Long, List<Object>> lists = new HashMap<>();
	private final Map<Long, Class<?>> chunkTypes = new HashMap<>();

	public AggregationChunkStorageStub(Eventloop eventloop, DefiningClassLoader classLoader) {
		this.eventloop = eventloop;
		this.classLoader = classLoader;
	}

	@Override
	public <T> StreamProducer<T> chunkReader(List<String> keys, List<String> fields,
	                                         Class<T> recordClass, long id, DefiningClassLoader classLoader) {
		Class<?> sourceClass = chunkTypes.get(id);
		AsmBuilder<Function> factory = new AsmBuilder(classLoader, Function.class);

		Expression result = let(constructor(recordClass));
		ExpressionSequence applyDef = sequence(result);
		for (String key : keys) {
			applyDef.add(set(
					getter(result, key),
					getter(cast(arg(0), sourceClass), key)));
		}
		for (String metric : fields) {
			applyDef.add(set(
					getter(result, metric),
					getter(cast(arg(0), sourceClass), metric)));
		}

		applyDef.add(result);
		factory.method("apply", applyDef);
		Function function = factory.newInstance();
		StreamProducers.OfIterator<T> chunkReader = (StreamProducers.OfIterator<T>) StreamProducers.ofIterable(eventloop,
				Iterables.transform(lists.get(id), function));
		chunkReader.setTag(id);
		return chunkReader;
	}

	@Override
	public <T> void chunkWriter(List<String> keys, List<String> fields, Class<T> recordClass, long id,
	                            StreamProducer<T> producer, DefiningClassLoader classLoader, CompletionCallback callback) {
		chunkTypes.put(id, recordClass);
		ArrayList<Object> list = new ArrayList<>();
		lists.put(id, list);
		StreamConsumers.ToList<Object> listConsumer = StreamConsumers.toList(eventloop, list);
		listConsumer.setCompletionCallback(callback);
		producer.streamTo((StreamConsumer<T>) listConsumer);
	}

	@Override
	public void removeChunk(long id, CompletionCallback callback) {
		chunkTypes.remove(id);
		lists.remove(id);
		callback.onComplete();
	}
}
