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

import java.lang.reflect.Array;
import java.util.List;

import static org.objectweb.asm.Type.getType;

final class ExpressionArrayNew implements Expression {
	private final Type type;
	private final List<Expression> lengths;

	ExpressionArrayNew(Class<?> type, List<Expression> lengths) {
		this.type = getType(Array.newInstance(type, new int[lengths.size()]).getClass());
		this.lengths = lengths;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		lengths.forEach(length -> length.load(ctx));
		if (type.getDimensions() == 1) {
			g.newArray(getType(type.getDescriptor().substring(1)));
		} else {
			g.visitMultiANewArrayInsn(type.getDescriptor(), lengths.size());
		}
		return type;
	}
}
