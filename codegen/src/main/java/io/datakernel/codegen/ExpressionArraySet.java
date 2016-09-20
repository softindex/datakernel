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

import static org.objectweb.asm.Type.getType;

final class ExpressionArraySet implements Expression {
	private final Expression array;
	private final Expression position;
	private final Expression newElement;

	ExpressionArraySet(Expression array, Expression position, Expression newElement) {
		this.array = array;
		this.position = position;
		this.newElement = newElement;
	}

	@Override
	public Type type(Context ctx) {
		return Type.VOID_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		array.load(ctx);
		position.load(ctx);
		newElement.load(ctx);
		g.arrayStore(getType(array.type(ctx).getDescriptor().substring(1)));
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionArraySet that = (ExpressionArraySet) o;

		if (array != null ? !array.equals(that.array) : that.array != null) return false;
		if (position != null ? !position.equals(that.position) : that.position != null) return false;
		return !(newElement != null ? !newElement.equals(that.newElement) : that.newElement != null);

	}

	@Override
	public int hashCode() {
		int result = array != null ? array.hashCode() : 0;
		result = 31 * result + (position != null ? position.hashCode() : 0);
		result = 31 * result + (newElement != null ? newElement.hashCode() : 0);
		return result;
	}
}
