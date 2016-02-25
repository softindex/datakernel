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

import io.datakernel.aggregation_db.fieldtype.HyperLogLog;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.VarField;

import static io.datakernel.codegen.Expressions.*;

public final class HyperLogLogFieldProcessor implements FieldProcessor {
	private final int registers;

	public HyperLogLogFieldProcessor(int registers) {
		this.registers = registers;
	}

	@Override
	public Expression getOnFirstItemExpression(VarField accumulator, Class<?> accumulatorClass,
	                                           VarField firstValue, Class<?> valueClass) {
		ExpressionSequence seq = sequence();
		seq.add(set(accumulator, constructor(HyperLogLog.class, value(registers))));
		seq.add(call(accumulator, "union", firstValue));
		return seq;
	}

	@Override
	public Expression getOnNextItemExpression(VarField accumulator, Class<?> accumulatorClass,
	                                          VarField nextValue, Class<?> valueClass) {
		return call(accumulator, "union", nextValue);
	}

	@Override
	public Expression getCreateAccumulatorExpression(VarField accumulator, Class<?> accumulatorClass,
	                                                 VarField firstValue, Class<?> valueClass) {
		ExpressionSequence seq = sequence();
		seq.add(set(accumulator, constructor(HyperLogLog.class, value(registers))));
		seq.add(getAddExpression(accumulator, firstValue, valueClass));
		return seq;
	}

	@Override
	public Expression getAccumulateExpression(VarField accumulator, Class<?> accumulatorClass,
	                                          VarField nextValue, Class<?> valueClass) {
		return getAddExpression(accumulator, nextValue, valueClass);
	}

	private static Expression getAddExpression(VarField accumulator, VarField value, Class<?> valueClass) {
		if (valueClass.isAssignableFrom(long.class) || valueClass.isAssignableFrom(Long.class))
			return call(accumulator, "addLong", value);

		if (valueClass.isAssignableFrom(int.class))
			return call(accumulator, "addInt", value);

		return call(accumulator, "addObject", value);
	}
}
