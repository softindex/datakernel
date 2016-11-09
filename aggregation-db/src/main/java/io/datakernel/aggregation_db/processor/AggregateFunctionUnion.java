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

import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.VarField;

import java.util.*;

import static io.datakernel.codegen.Expressions.*;

public final class AggregateFunctionUnion extends AggregateFunction {
	AggregateFunctionUnion(FieldType fieldType) {
		super(fieldType);
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return accumulator;
	}

	@Override
	public Expression zeroAccumulator(VarField accumulator) {
		return sequence(getInitializeExpression(accumulator));
	}

	@Override
	public Expression initAccumulatorWithAccumulator(VarField accumulator,
	                                                 Expression firstAccumulator) {
		return sequence(
				getInitializeExpression(accumulator),
				call(accumulator, "addAll", cast(firstAccumulator, Collection.class)));
	}

	@Override
	public Expression reduce(VarField accumulator,
	                         VarField nextAccumulator) {
		return call(accumulator, "addAll", cast(nextAccumulator, Collection.class));
	}

	@Override
	public Expression initAccumulatorWithValue(VarField accumulator,
	                                           VarField firstValue) {
		List<Expression> expressions = new ArrayList<>();
		expressions.add(getInitializeExpression(accumulator));
		expressions.add(call(accumulator, "add", cast(firstValue, Object.class)));
		return sequence(expressions);
	}

	@Override
	public Expression accumulate(VarField accumulator,
	                             VarField nextValue) {
		return call(accumulator, "add", cast(nextValue, Object.class));
	}

	private Expression getInitializeExpression(VarField accumulator) {
		Class<?> accumulatorClass = fieldType.getInternalDataType();
		if (accumulatorClass.isAssignableFrom(List.class))
			return set(accumulator, constructor(ArrayList.class));

		if (accumulatorClass.isAssignableFrom(Set.class))
			return set(accumulator, constructor(LinkedHashSet.class));

		throw new IllegalArgumentException("Unsupported type");
	}
}
