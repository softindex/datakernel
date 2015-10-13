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
	public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id) {
		Class<?> sourceClass = chunkTypes.get(id);
		AsmBuilder<Function> factory = new AsmBuilder(classLoader, Function.class);

		Expression result = let(constructor(recordClass));
		ExpressionSequence applyDef = sequence(result);
		for (String key : keys) {
			applyDef.add(set(
					field(result, key),
					field(cast(arg(0), sourceClass), key)));
		}

//		applyDef.add(set(
//				field(result, "list"),
//				field(cast(arg(0), sourceClass), "list")
//		));
		for (String metric : fields) {
			applyDef.add(set(
					field(result, metric),
					field(cast(arg(0), sourceClass), metric)));
		}

		applyDef.add(result);
		factory.method("apply", applyDef);
		Function function = factory.newInstance();
		StreamProducers.OfIterator<T> chunkReader = (StreamProducers.OfIterator<T>) StreamProducers.ofIterable(eventloop, Iterables.transform(lists.get(id), function));
		chunkReader.setTag(id);
		return chunkReader;
	}

	@Override
	public <T> StreamConsumer<T> chunkWriter(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass, long id) {
		chunkTypes.put(id, recordClass);
		ArrayList<Object> list = new ArrayList<>();
		lists.put(id, list);
		return (StreamConsumer<T>) StreamConsumers.toList(eventloop, list);
	}

	@Override
	public void removeChunk(String aggregationId, long id) {
		chunkTypes.remove(id);
		lists.remove(id);
	}
}
