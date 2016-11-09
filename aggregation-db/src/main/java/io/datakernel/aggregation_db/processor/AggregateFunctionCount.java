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
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.VarField;

import static io.datakernel.codegen.Expressions.*;

public final class AggregateFunctionCount extends AggregateFunction {
	AggregateFunctionCount(FieldType fieldType) {
		super(fieldType);
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return accumulator;
	}

	@Override
	public Expression zeroAccumulator(VarField accumulator) {
		return Expressions.voidExp();
	}

	@Override
	public Expression initAccumulatorWithAccumulator(VarField accumulator,
	                                                 Expression firstAccumulator) {
		return set(accumulator, firstAccumulator);
	}

	@Override
	public Expression reduce(VarField accumulator,
	                         VarField nextAccumulator) {
		return set(accumulator, add(accumulator, nextAccumulator));
	}

	@Override
	public Expression initAccumulatorWithValue(VarField accumulator,
	                                           VarField firstValue) {
		return set(accumulator, value(1));
	}

	@Override
	public Expression accumulate(VarField accumulator,
	                             VarField nextValue) {
		return set(accumulator, inc(accumulator));
	}
}
