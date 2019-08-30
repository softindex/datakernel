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

import static io.datakernel.codegen.Expressions.newLocal;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.Type.getType;

final class ExpressionSwitchByIndex implements Expression {
	private final Expression index;
	private final List<Expression> expressions;
	private final Expression defaultExpression;

	ExpressionSwitchByIndex(Expression index, List<Expression> expressions, Expression defaultExpression) {
		this.index = index;
		this.expressions = expressions;
		this.defaultExpression = defaultExpression;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		VarLocal index = newLocal(ctx, this.index.load(ctx));
		index.store(ctx);

		Label labelExit = new Label();

		Type listItemType = getType(Object.class);

		for (int i = 0; i < expressions.size(); i++) {
			Label labelNext = new Label();

			g.push(i);
			index.load(ctx);
			g.ifCmp(INT_TYPE, GeneratorAdapter.NE, labelNext);

			listItemType = expressions.get(i).load(ctx);
			g.goTo(labelExit);

			g.mark(labelNext);
		}

		defaultExpression.load(ctx);

		g.mark(labelExit);

		return listItemType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionSwitchByIndex that = (ExpressionSwitchByIndex) o;

		if (!Objects.equals(index, that.index)) return false;
		if (!Objects.equals(expressions, that.expressions)) return false;
		if (!Objects.equals(defaultExpression, that.defaultExpression)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = index.hashCode();
		result = 31 * result + expressions.hashCode();
		result = 31 * result + (defaultExpression == null ? 0 : expressions.hashCode());
		return result;
	}
}
