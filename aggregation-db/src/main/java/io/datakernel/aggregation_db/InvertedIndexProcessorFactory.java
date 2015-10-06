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

import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.stream.processor.StreamReducers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;

public class InvertedIndexProcessorFactory implements ProcessorFactory {
	private final DefiningClassLoader classLoader;

	public InvertedIndexProcessorFactory(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                                 List<String> inputFields, List<String> outputFields) {
		AsmBuilder<StreamReducers.Reducer> builder = new AsmBuilder<>(classLoader, StreamReducers.Reducer.class);

		String listFieldName = outputFields.get(0);
		String addAllMethodName = "addAll";

		// Define the function that constructs the accumulator.
		Expression accumulator = let(constructor(outputClass));
		ExpressionSequence onFirstItemDef = sequence(accumulator);
		onFirstItemDef.add(set(field(accumulator, listFieldName),
				constructor(ArrayList.class))); // call ArrayList constructor
		// Copy names and values of keys from the record passed as the third argument
		for (String key : keys) {
			onFirstItemDef.add(set(
					field(accumulator, key),
					field(cast(arg(2), inputClass), key)));
		}
		// Call <listName>.addAll(firstValue.<listName>);
		onFirstItemDef.add(call(
				field(accumulator, listFieldName),
				addAllMethodName,
				cast(field(cast(arg(2), inputClass), listFieldName), Collection.class)
		));
		onFirstItemDef.add(accumulator);
		builder.method("onFirstItem", onFirstItemDef);

		// Call <listName>.addAll(nextValue.<listName>);
		ExpressionSequence onNextItemDef = sequence();
		onNextItemDef.add(call(
				field(cast(arg(3), outputClass), listFieldName),
				addAllMethodName,
				cast(field(cast(arg(2), inputClass), listFieldName), Collection.class)
		));
		onNextItemDef.add(arg(3));
		builder.method("onNextItem", onNextItemDef);

		// Pass the accumulator object to the data receiver.
		builder.method("onComplete", call(arg(0), "onData", arg(2)));

		return builder.newInstance();
	}

	@Override
	public Aggregate createPreaggregator(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                     List<String> inputFields, List<String> outputFields) {
		AsmBuilder<Aggregate> builder = new AsmBuilder<>(classLoader, Aggregate.class);

		String listFieldName = outputFields.get(0);
		String addMethodName = "add";

		Expression result = let(constructor(outputClass));
		ExpressionSequence createAccumulatorDef = sequence(result);

		// call ArrayList constructor
		createAccumulatorDef.add(set(field(result, listFieldName), constructor(ArrayList.class)));
		// Copy names and values of keys from the record passed as the third argument
		for (String key : keys) {
			createAccumulatorDef.add(set(
					field(result, key),
					field(cast(arg(0), inputClass), key)));
		}

		// Call <listName>.add(record.<valueName>)
		createAccumulatorDef.add(call(
				field(result, listFieldName),
				addMethodName,
				cast(field(cast(arg(0), inputClass), inputFields.get(0)), Object.class)
		));
		createAccumulatorDef.add(result);
		builder.method("createAccumulator", createAccumulatorDef);

		// Call <listName>.add(record.<valueName>)
		builder.method("accumulate", call(
				field(cast(arg(0), outputClass), listFieldName),
				addMethodName,
				cast(field(cast(arg(1), inputClass), inputFields.get(0)), Object.class)
		));

		return builder.newInstance();
	}
}
