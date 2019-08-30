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

import static io.datakernel.util.Preconditions.checkNotNull;

final class ExpressionArrayGet implements Expression {
	private final Expression array;
	private final Expression index;

	ExpressionArrayGet(Expression array, Expression index) {
		this.array = checkNotNull(array);
		this.index = checkNotNull(index);
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionArrayGet that = (ExpressionArrayGet) o;

		if (!array.equals(that.array)) return false;
		if (!index.equals(that.index)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = array.hashCode();
		result = 31 * result + index.hashCode();
		return result;
	}
}
