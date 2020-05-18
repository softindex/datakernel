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

package io.datakernel.codegen.expression;

import io.datakernel.codegen.Context;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

final class ExpressionArrayGet implements Expression {
	private final Expression array;
	private final Expression index;

	ExpressionArrayGet(Expression array, Expression index) {
		this.array = array;
		this.index = index;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type arrayType = array.load(ctx);
		Type type = Type.getType(arrayType.getDescriptor().substring(1));
		index.load(ctx);
		g.arrayLoad(type);
		return type;
	}
}
