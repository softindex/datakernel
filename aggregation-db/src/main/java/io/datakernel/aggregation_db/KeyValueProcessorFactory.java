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
import io.datakernel.codegen.PredicateDefCmp;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.stream.processor.StreamReducers;

import java.util.List;

import static io.datakernel.codegen.Expressions.*;

public class KeyValueProcessorFactory implements ProcessorFactory {
	private final DefiningClassLoader classLoader;

	public KeyValueProcessorFactory(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                                 List<String> inputFields, List<String> outputFields) {
		AsmBuilder<StreamReducers.Reducer> builder = new AsmBuilder<>(classLoader,
				StreamReducers.Reducer.class);

		/* Define the function that creates the accumulator object
		by copying all properties from object in the third argument.
		*/
		Expression accumulator = let(constructor(outputClass));
		ExpressionSequence onFirstItemDef = sequence(accumulator);
		for (String key : keys) {
			onFirstItemDef.add(set(
					field(accumulator, key),
					field(cast(arg(2), inputClass), key)));
		}
		for (String field : inputFields) {
			onFirstItemDef.add(set(
					field(accumulator, field),
					field(cast(arg(2), inputClass), field)));
		}
		onFirstItemDef.add(accumulator);
		builder.method("onFirstItem", onFirstItemDef);

		/* Define the function that accumulates record specified in the third argument
		to the accumulator in the fourth argument.
		Specifically, we generate a code that compares timestamps of the accumulator
		and the record and replace the value in the accumulator if the record has later timestamp.
		*/
		ExpressionSequence onNextItemDef = sequence();
		for (String field : inputFields) {
			onNextItemDef.add(ifTrue(cmp(PredicateDefCmp.Operation.LT,
					field(cast(arg(3), outputClass), "timestamp"),
					field(cast(arg(2), inputClass), "timestamp")),
					set(field(cast(arg(3), outputClass), field), field(cast(arg(2), inputClass), field))));
		}
		onNextItemDef.add(arg(3));
		builder.method("onNextItem", onNextItemDef);

		// Define the function that passes the accumulator object to the data receiver.
		builder.method("onComplete", call(arg(0), "onData", arg(2)));

		return builder.newInstance();
	}

	@Override
	public Aggregate createPreaggregator(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                     List<String> inputFields, List<String> outputFields) {
		AsmBuilder<Aggregate> builder = new AsmBuilder<>(classLoader, Aggregate.class);

		/*
		Define the function that creates the accumulator object
		by copying all properties from object in the first argument.
		*/
		Expression result = let(constructor(outputClass));
		ExpressionSequence createAccumulatorDef = sequence(result);
		for (String key : keys) {
			createAccumulatorDef.add(set(
					field(result, key),
					field(cast(arg(0), inputClass), key)));
		}
		for (String field : inputFields) {
			createAccumulatorDef.add(set(
					field(result, field),
					field(cast(arg(0), inputClass), field)));
		}
		createAccumulatorDef.add(result);
		builder.method("createAccumulator", createAccumulatorDef);

		/*
		Define the function that accumulates record specified in the second argument
		to the accumulator in the first argument.
		Specifically, we generate a code that compares timestamps of the accumulator
		and the record and replace the value in the accumulator if the record has later timestamp.
		*/
		ExpressionSequence accumulateDef = sequence();
		for (String field : inputFields) {
			accumulateDef.add(
					ifTrue(cmp(PredicateDefCmp.Operation.LT,
							field(cast(arg(0), outputClass), "timestamp"),
							field(cast(arg(1), inputClass), "timestamp")),
							set(field(cast(arg(0), outputClass), field), field(cast(arg(1), inputClass), field))));
		}
		builder.method("accumulate", accumulateDef);

		return builder.newInstance();
	}
}
