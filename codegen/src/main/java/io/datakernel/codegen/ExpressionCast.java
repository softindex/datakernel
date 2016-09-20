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

package io.datakernel.codegen;

import org.objectweb.asm.Type;

import static io.datakernel.codegen.Utils.loadAndCast;
import static org.objectweb.asm.Type.getType;

/**
 * Defines method in order to cast a function to a type
 */
final class ExpressionCast implements Expression {
	public static final Type THIS_TYPE = getType(Object.class);

	private final Expression expression;
	private final Type targetType;

	ExpressionCast(Expression expression, Type type) {
		this.expression = expression;
		this.targetType = type;
	}

	@Override
	public Type type(Context ctx) {
		return targetType == THIS_TYPE ? ctx.getThisType() : targetType;
	}

	@Override
	public Type load(Context ctx) {
		Type targetType = type(ctx);
		loadAndCast(ctx, expression, targetType);
		return targetType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionCast that = (ExpressionCast) o;

		if (expression != null ? !expression.equals(that.expression) : that.expression != null) return false;
		if (targetType != null ? !targetType.equals(that.targetType) : that.targetType != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = expression != null ? expression.hashCode() : 0;
		result = 31 * result + (targetType != null ? targetType.hashCode() : 0);
		return result;
	}
}
