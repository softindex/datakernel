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

package io.datakernel.aggregation_db.processor;

import io.datakernel.aggregation_db.Aggregate;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.stream.processor.StreamReducers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.datakernel.codegen.Expressions.*;

public final class ProcessorFactory {
	private final AggregationStructure structure;

	private ProcessorFactory(AggregationStructure structure) {
		this.structure = structure;
	}

	public static ProcessorFactory create(AggregationStructure structure) {return new ProcessorFactory(structure);}

	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                                 List<String> fields, DefiningClassLoader classLoader) {

		Expression accumulator = let(constructor(outputClass));
		List<Expression> onFirstItemDefExpressions = new ArrayList<>();
		onFirstItemDefExpressions.add(accumulator);
		List<Expression> onNextItemDefExpressions = new ArrayList<>();

		for (String key : keys) {
			onFirstItemDefExpressions.add(set(
					getter(accumulator, key),
					getter(cast(arg(2), inputClass), key)));
		}

		for (String field : fields) {
			FieldType fieldType = structure.getFieldType(field);
			Class<?> accumulatorClass = fieldType.getDataType();
			FieldProcessor fieldProcessor = fieldType.fieldProcessor();
			onFirstItemDefExpressions.add(fieldProcessor
					.getOnFirstItemExpression(
							getter(accumulator, field),
							accumulatorClass,
							getter(cast(arg(2), inputClass), field),
							accumulatorClass));
			onNextItemDefExpressions.add(fieldProcessor
					.getOnNextItemExpression(
							getter(cast(arg(3), outputClass), field),
							accumulatorClass,
							getter(cast(arg(2), inputClass), field),
							accumulatorClass));
		}

		onFirstItemDefExpressions.add(accumulator);
		ClassBuilder<StreamReducers.Reducer> builder = ClassBuilder.create(classLoader, StreamReducers.Reducer.class)
				.withMethod("onFirstItem", sequence(onFirstItemDefExpressions));

		onNextItemDefExpressions.add(arg(3));
		return builder
				.withMethod("onNextItem", sequence(onNextItemDefExpressions))
				.withMethod("onComplete", call(arg(0), "onData", arg(2)))
				.buildClassAndCreateNewInstance();
	}

	public Aggregate createPreaggregator(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                     List<String> fields, Map<String, String> outputToInputFields,
	                                     DefiningClassLoader classLoader) {

		Expression accumulator = let(constructor(outputClass));
//		ExpressionSequence createAccumulatorDef = sequence(accumulator);
//		ExpressionSequence accumulateDef = sequence();
		List<Expression> createAccumulatorDefExpressions = new ArrayList<>();
		createAccumulatorDefExpressions.add(accumulator);
		List<Expression> accumulateDefExpressions = new ArrayList<>();

		for (String key : keys) {
			createAccumulatorDefExpressions.add(set(
					getter(accumulator, key),
					getter(cast(arg(0), inputClass), key)));
		}

		for (String outputField : fields) {
			String inputField = outputToInputFields != null && outputToInputFields.containsKey(outputField) ?
					outputToInputFields.get(outputField) : outputField;
			createAggregateExpressions(accumulator, inputField, inputClass, outputField, outputClass,
					createAccumulatorDefExpressions, accumulateDefExpressions);
		}

		createAccumulatorDefExpressions.add(accumulator);

		return ClassBuilder.create(classLoader, Aggregate.class)
				.withMethod("createAccumulator", sequence(createAccumulatorDefExpressions))
				.withMethod("accumulate", sequence(accumulateDefExpressions))
				.buildClassAndCreateNewInstance();
	}

	private void createAggregateExpressions(Expression accumulator, String inputField, Class<?> inputClass,
	                                        String outputField, Class<?> outputClass,
	                                        List<Expression> createAccumulatorDefExpressions,
	                                        List<Expression> accumulateDefExpressions) {
		FieldType fieldType = structure.getFieldType(outputField);
		Class<?> accumulatorClass = fieldType.getDataType();
		FieldProcessor fieldProcessor = fieldType.fieldProcessor();
		Class<?> valueClass = inputField == null ? null : getType(inputClass, inputField);

		createAccumulatorDefExpressions.add(fieldProcessor.getCreateAccumulatorExpression(
				getter(accumulator, outputField),
				accumulatorClass,
				inputField == null ? null : getter(cast(arg(0), inputClass), inputField),
				valueClass));
		accumulateDefExpressions.add(fieldProcessor.getAccumulateExpression(
				getter(cast(arg(0), outputClass), outputField),
				accumulatorClass,
				inputField == null ? null : getter(cast(arg(1), inputClass), inputField),
				valueClass));
	}

	private static Class<?> getType(Class<?> c, String field) {
		try {
			return c.getDeclaredField(field).getType();
		} catch (NoSuchFieldException e) {
			throw new IllegalArgumentException("Given class does not contain field with the specified name");
		}
	}
}
