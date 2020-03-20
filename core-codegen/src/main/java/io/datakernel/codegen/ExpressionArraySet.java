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

import org.jetbrains.annotations.NotNull;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

import static org.objectweb.asm.Type.getType;

final class ExpressionArraySet implements Expression {
	private final Expression array;
	private final List<Expression> positions;
	private final Expression newElement;

	ExpressionArraySet(@NotNull Expression array, @NotNull List<Expression> positions, @NotNull Expression newElement) {
		this.array = array;
		this.positions = positions;
		this.newElement = newElement;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		String descriptor = array.load(ctx).getDescriptor();
		int dimensions = positions.size();
		for (int i = 0; i < dimensions - 1; i++) {
			positions.get(i).load(ctx);
			g.arrayLoad(getType(descriptor.substring(i + 1)));
		}
		positions.get(dimensions - 1).load(ctx);
		newElement.load(ctx);
		g.arrayStore(getType(descriptor.substring(dimensions)));
		return Type.VOID_TYPE;
	}
}
