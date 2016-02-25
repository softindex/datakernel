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
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.VarField;

import java.util.*;

import static io.datakernel.codegen.Expressions.*;

public final class CollectionFieldProcessor implements FieldProcessor {
	@Override
	public Expression getOnFirstItemExpression(VarField accumulator, Class<?> accumulatorClass,
	                                           VarField firstValue, Class<?> valueClass) {
		ExpressionSequence seq = sequence();
		seq.add(getInitializeExpression(accumulator, accumulatorClass));
		seq.add(call(accumulator, "addAll", cast(firstValue, Collection.class)));
		return seq;
	}

	@Override
	public Expression getOnNextItemExpression(VarField accumulator, Class<?> accumulatorClass,
	                                          VarField nextValue, Class<?> valueClass) {
		return call(accumulator, "addAll", cast(nextValue, Collection.class));
	}

	@Override
	public Expression getCreateAccumulatorExpression(VarField accumulator, Class<?> accumulatorClass,
	                                                 VarField firstValue, Class<?> valueClass) {
		ExpressionSequence seq = sequence();
		seq.add(getInitializeExpression(accumulator, accumulatorClass));
		seq.add(call(accumulator, "add", cast(firstValue, Object.class)));
		return seq;
	}

	@Override
	public Expression getAccumulateExpression(VarField accumulator, Class<?> accumulatorClass,
	                                          VarField nextValue, Class<?> valueClass) {
		return call(accumulator, "add", cast(nextValue, Object.class));
	}

	private static Expression getInitializeExpression(VarField accumulator, Class<?> accumulatorClass) {
		if (accumulatorClass.isAssignableFrom(List.class))
			return set(accumulator, constructor(ArrayList.class));

		if (accumulatorClass.isAssignableFrom(Set.class))
			return set(accumulator, constructor(HashSet.class));

		throw new IllegalArgumentException("Unsupported type");
	}
}
