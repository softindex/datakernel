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

import static io.datakernel.codegen.Utils.newLocal;
import static io.datakernel.util.Preconditions.checkNotNull;

final class ExpressionFor implements Expression {
	private final Expression from;
	private final Expression to;
	private final Expression forVar;

	ExpressionFor(Expression from, Expression to, Expression forVar) {
		this.from = checkNotNull(from);
		this.to = checkNotNull(to);
		this.forVar = checkNotNull(forVar);
	}

	@Override
	public Type type(Context ctx) {
		return Type.VOID_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		VarLocal to = newLocal(ctx, Type.INT_TYPE);
		this.to.load(ctx);
		to.store(ctx);

		from.load(ctx);
		VarLocal varIt = newLocal(ctx, Type.INT_TYPE);
		varIt.store(ctx);

		g.mark(labelLoop);

		varIt.load(ctx);
		to.load(ctx);

		g.ifCmp(Type.INT_TYPE, GeneratorAdapter.GE, labelExit);

		ctx.addParameter("it", varIt);
		forVar.load(ctx);

		varIt.load(ctx);
		g.push(1);
		g.math(GeneratorAdapter.ADD, Type.INT_TYPE);
		varIt.store(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);

		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionFor that = (ExpressionFor) o;

		if (!to.equals(that.to)) return false;
		if (!from.equals(that.from)) return false;
		if (!forVar.equals(that.forVar)) return false;

		return true;

	}

	@Override
	public int hashCode() {
		int result = to.hashCode();
		result = 31 * result + from.hashCode();
		result = 31 * result + forVar.hashCode();
		return result;
	}
}