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

import java.util.function.Function;

import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.Type.VOID_TYPE;

final class ExpressionFor implements Expression {
	private final Expression from;
	private final Expression to;
	private final Function<Expression, Expression> forVar;

	ExpressionFor(Expression from, Expression to, Function<Expression, Expression> forVar) {
		this.from = from;
		this.to = to;
		this.forVar = forVar;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		VarLocal to = ctx.newLocal(INT_TYPE);
		this.to.load(ctx);
		to.store(ctx);

		from.load(ctx);
		VarLocal it = ctx.newLocal(INT_TYPE);
		it.store(ctx);

		g.mark(labelLoop);

		it.load(ctx);
		to.load(ctx);

		g.ifCmp(INT_TYPE, GeneratorAdapter.GE, labelExit);

		Type forType = forVar.apply(it).load(ctx);
		if (forType.getSize() == 1)
			g.pop();
		if (forType.getSize() == 2)
			g.pop2();

		it.load(ctx);
		g.push(1);
		g.math(GeneratorAdapter.ADD, INT_TYPE);
		it.store(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);

		return VOID_TYPE;
	}
}
