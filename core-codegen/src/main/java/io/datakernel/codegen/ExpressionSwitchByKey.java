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

package io.datakernel.codegen;

import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;
import java.util.Objects;

import static io.datakernel.codegen.Expressions.exception;
import static io.datakernel.codegen.Expressions.newLocal;
import static io.datakernel.codegen.Utils.isPrimitiveType;
import static org.objectweb.asm.Type.getType;

final class ExpressionSwitchByKey implements Expression {
	public static final Expression DEFAULT_EXPRESSION = exception(IllegalArgumentException.class);

	private final Expression value;
	private final List<Expression> matchCases;
	private final List<Expression> matchExpressions;
	private final Expression defaultExpression;

	ExpressionSwitchByKey(Expression value, List<Expression> matchCases, List<Expression> matchExpressions, Expression defaultExpression) {
		this.value = value;
		this.matchCases = matchCases;
		this.matchExpressions = matchExpressions;
		this.defaultExpression = defaultExpression;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type keyType = this.value.load(ctx);
		VarLocal value = newLocal(ctx, keyType);
		value.store(ctx);

		Label labelExit = new Label();

		Type resultType = getType(Object.class);
		for (int i = 0; i < matchCases.size(); i++) {
			Label labelNext = new Label();
			if (isPrimitiveType(keyType)) {
				matchCases.get(i).load(ctx);
				value.load(ctx);
				g.ifCmp(keyType, GeneratorAdapter.NE, labelNext);
			} else {
				ctx.invoke(matchCases.get(i), "equals", value);
				g.push(true);
				g.ifCmp(Type.BOOLEAN_TYPE, GeneratorAdapter.NE, labelNext);
			}

			resultType = matchExpressions.get(i).load(ctx);
			g.goTo(labelExit);

			g.mark(labelNext);
		}

		defaultExpression.load(ctx);

		g.mark(labelExit);

		return resultType;
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionSwitchByKey that = (ExpressionSwitchByKey) o;

		if (!Objects.equals(this.value, that.value)) return false;
		if (!Objects.equals(this.defaultExpression, that.defaultExpression)) return false;
		if (!Objects.equals(this.matchCases, that.matchCases)) return false;
		if (!Objects.equals(this.matchExpressions, that.matchExpressions)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		return Objects.hash(value, defaultExpression, matchCases, matchExpressions);
	}
}
