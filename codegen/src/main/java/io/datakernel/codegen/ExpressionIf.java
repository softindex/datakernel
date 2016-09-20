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

import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

final class ExpressionIf implements Expression {
	private final PredicateDef condition;
	private final Expression left;
	private final Expression right;

	ExpressionIf(PredicateDef condition, Expression left, Expression right) {
		this.condition = condition;
		this.left = left;
		this.right = right;
	}

	@Override
	public Type type(Context ctx) {
		return left.type(ctx);
	}

	@Override
	public Type load(Context ctx) {
		Label labelTrue = new Label();
		Label labelExit = new Label();

		GeneratorAdapter g = ctx.getGeneratorAdapter();
		condition.load(ctx);
		g.push(true);

		g.ifCmp(condition.type(ctx), GeneratorAdapter.EQ, labelTrue);

		if (right != null) {
			right.load(ctx);
		}

		g.goTo(labelExit);

		g.mark(labelTrue);
		left.load(ctx);

		g.mark(labelExit);
		return left.type(ctx);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionIf that = (ExpressionIf) o;

		if (condition != null ? !condition.equals(that.condition) : that.condition != null) return false;
		if (left != null ? !left.equals(that.left) : that.left != null) return false;
		return !(right != null ? !right.equals(that.right) : that.right != null);

	}

	@Override
	public int hashCode() {
		int result = condition != null ? condition.hashCode() : 0;
		result = 31 * result + (left != null ? left.hashCode() : 0);
		result = 31 * result + (right != null ? right.hashCode() : 0);
		return result;
	}
}
