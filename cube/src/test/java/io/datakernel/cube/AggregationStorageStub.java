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

package io.datakernel.cube;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.codegen.AsmFunctionFactory;
import io.datakernel.codegen.FunctionDefSequence;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.codegen.FunctionDefs.*;

@SuppressWarnings("unchecked")
public class AggregationStorageStub implements AggregationStorage {
	private final Eventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final Map<Long, List<Object>> lists = new HashMap<>();
	private final Map<Long, Class<?>> chunkTypes = new HashMap<>();

	public AggregationStorageStub(Eventloop eventloop, DefiningClassLoader classLoader) {
		this.eventloop = eventloop;
		this.classLoader = classLoader;
	}

	@Override
	public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> dimensions, List<String> measures, Class<T> recordClass, long id) {
		Class<?> sourceClass = chunkTypes.get(id);
		AsmFunctionFactory<Function> factory = new AsmFunctionFactory(classLoader, Function.class);
		FunctionDefSequence applyDef = sequence(let("RESULT", constructor(recordClass)));
		for (String dimension : dimensions) {
			applyDef.add(set(
					field(var("RESULT"), dimension),
					field(cast(arg(0), sourceClass), dimension)));
		}
		for (String metric : measures) {
			applyDef.add(set(
					field(var("RESULT"), metric),
					field(cast(arg(0), sourceClass), metric)));
		}
		applyDef.add(var("RESULT"));
		factory.method("apply", applyDef);
		Function function = factory.newInstance();
		return (StreamProducer<T>) StreamProducers.ofIterable(eventloop, Iterables.transform(lists.get(id), function));
	}

	@Override
	public <T> StreamConsumer<T> chunkWriter(String aggregationId, List<String> dimensions, List<String> measures, Class<T> recordClass, long id) {
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
