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

package io.datakernel.aggregation.measure;

import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;

import static io.datakernel.codegen.Expressions.*;

public final class MeasureMax extends Measure {
	@SuppressWarnings("rawtypes")
	MeasureMax(FieldType fieldType) {
		super(fieldType);
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return accumulator;
	}

	@Override
	public Expression zeroAccumulator(Variable accumulator) {
		return voidExp();
	}

	@Override
	public Expression initAccumulatorWithAccumulator(Variable accumulator, Expression firstAccumulator) {
		return set(accumulator, firstAccumulator);
	}

	@Override
	public Expression reduce(Variable accumulator,
			Variable nextAccumulator) {
		return set(accumulator, staticCall(Math.class, "max", accumulator, nextAccumulator));
	}

	@Override
	public Expression initAccumulatorWithValue(Variable accumulator,
			Variable firstValue) {
		return set(accumulator, firstValue);
	}

	@Override
	public Expression accumulate(Variable accumulator,
			Variable nextValue) {
		return set(accumulator, staticCall(Math.class, "max", accumulator, nextValue));
	}
}
