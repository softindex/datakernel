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

import java.util.List;

final class ExpressionArrayGet implements Expression {
	private final Expression array;
	private final List<Expression> indexes;

	ExpressionArrayGet(Expression array, List<Expression> indexes) {
		this.array = array;
		this.indexes = indexes;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		String descriptor = array.load(ctx).getDescriptor();
		Type type = null;
		for (int i = 0; i < indexes.size(); i++) {
			type = Type.getType(descriptor.substring(i + 1));
			indexes.get(i).load(ctx);
			g.arrayLoad(type);
		}
		return type;
	}
}
