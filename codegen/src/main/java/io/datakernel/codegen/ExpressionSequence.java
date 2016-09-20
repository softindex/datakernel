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
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.ArrayList;
import java.util.List;

import static org.objectweb.asm.Type.VOID_TYPE;

/**
 * Defines methods which allow to use several methods one after the other
 */
@SuppressWarnings("PointlessArithmeticExpression")
final class ExpressionSequence implements Expression {
	private final List<Expression> expressions = new ArrayList<>();

	public ExpressionSequence(List<Expression> expressions) {
		this.expressions.addAll(expressions);
	}

	public ExpressionSequence add(Expression expression) {
		expressions.add(expression);
		return this;
	}

	@Override
	public Type type(Context ctx) {
		Expression expression = getLast(expressions);
		return expression.type(ctx);
	}

	private Expression getLast(List<Expression> list) {
		return list.get(list.size() - 1);
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type type = VOID_TYPE;
		for (int i = 0; i < expressions.size(); i++) {
			Expression expression = expressions.get(i);
			type = expression.load(ctx);
			if (i != expressions.size() - 1) {
				if (type.getSize() == 1)
					g.pop();
				if (type.getSize() == 2)
					g.pop2();
			}
		}

		return type;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionSequence that = (ExpressionSequence) o;

		return expressions.equals(that.expressions);
	}

	@Override
	public int hashCode() {
		return expressions.hashCode();
	}
}
