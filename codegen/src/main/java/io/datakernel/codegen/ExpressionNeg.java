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

import static org.objectweb.asm.Type.*;

public class ExpressionNeg implements Expression {
	private final Expression arg;

	ExpressionNeg(Expression arg) {this.arg = arg;}

	@Override
	public Type type(Context ctx) {
		switch (arg.type(ctx).getSort()) {
			case Type.BOOLEAN:
				return BOOLEAN_TYPE;
			case Type.BYTE:
			case Type.SHORT:
			case Type.CHAR:
			case Type.INT:
				return INT_TYPE;
			case Type.LONG:
				return LONG_TYPE;
			case Type.FLOAT:
				return FLOAT_TYPE;
			case Type.DOUBLE:
				return DOUBLE_TYPE;
			default:
				throw new IllegalArgumentException();
		}
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		int sort = arg.type(ctx).getSort();
		arg.load(ctx);

		if (sort == Type.DOUBLE || sort == Type.FLOAT || sort == Type.LONG || sort == Type.INT) {
			g.math(GeneratorAdapter.NEG, arg.type(ctx));
			return arg.type(ctx);
		}
		if (sort == Type.BYTE || sort == Type.SHORT || sort == Type.CHAR) {
			g.cast(arg.type(ctx), INT_TYPE);
			g.math(GeneratorAdapter.NEG, INT_TYPE);
			return arg.type(ctx);
		}

		if (sort == Type.BOOLEAN) {
			Label labelTrue = new Label();
			Label labelExit = new Label();
			g.push(true);
			g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelTrue);
			g.push(true);
			g.goTo(labelExit);

			g.mark(labelTrue);
			g.push(false);

			g.mark(labelExit);
			return INT_TYPE;
		}

		throw new IllegalArgumentException();
	}
}
