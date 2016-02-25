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

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.VarField;

import static io.datakernel.codegen.Expressions.callStatic;
import static io.datakernel.codegen.Expressions.set;

public final class MinFieldProcessor implements FieldProcessor {
	@Override
	public Expression getOnFirstItemExpression(VarField accumulator, Class<?> accumulatorClass,
	                                           VarField firstValue, Class<?> valueClass) {
		return set(accumulator, firstValue);
	}

	@Override
	public Expression getOnNextItemExpression(VarField accumulator, Class<?> accumulatorClass,
	                                          VarField nextValue, Class<?> valueClass) {
		return set(accumulator, callStatic(Math.class, "min", accumulator, nextValue));
	}

	@Override
	public Expression getCreateAccumulatorExpression(VarField accumulator, Class<?> accumulatorClass,
	                                                 VarField firstValue, Class<?> valueClass) {
		return set(accumulator, firstValue);
	}

	@Override
	public Expression getAccumulateExpression(VarField accumulator, Class<?> accumulatorClass,
	                                          VarField nextValue, Class<?> valueClass) {
		return set(accumulator, callStatic(Math.class, "min", accumulator, nextValue));
	}
}
