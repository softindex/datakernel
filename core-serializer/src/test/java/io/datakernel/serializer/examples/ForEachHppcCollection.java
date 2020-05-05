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

package io.datakernel.serializer.examples;

import io.datakernel.codegen.expression.AbstractExpressionIteratorForEach;
import io.datakernel.codegen.expression.Expression;
import io.datakernel.codegen.expression.VarLocal;

import java.util.function.Function;

import static io.datakernel.codegen.expression.Expressions.property;

public final class ForEachHppcCollection extends AbstractExpressionIteratorForEach {
	public ForEachHppcCollection(Expression collection, Class<?> type, Function<Expression, Expression> forEach) {
		super(collection, type, forEach);
	}

	@Override
	protected Expression getValue(VarLocal varIt) {
		return property(varIt, "value");
	}
}
