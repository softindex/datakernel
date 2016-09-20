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

final class ExpressionArrayGet implements Expression {
	private final Expression array;
	private final Expression nom;

	ExpressionArrayGet(Expression array, Expression nom) {
		this.array = array;
		this.nom = nom;
	}

	@Override
	public Type type(Context ctx) {
		return Type.getType(array.type(ctx).getDescriptor().substring(1));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionArrayGet that = (ExpressionArrayGet) o;

		if (array != null ? !array.equals(that.array) : that.array != null) return false;
		return !(nom != null ? !nom.equals(that.nom) : that.nom != null);

	}

	@Override
	public int hashCode() {
		int result = array != null ? array.hashCode() : 0;
		result = 31 * result + (nom != null ? nom.hashCode() : 0);
		return result;
	}

	@Override
	public Type load(Context ctx) {
		Type type = Type.getType(array.type(ctx).getDescriptor().substring(1));
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		array.load(ctx);
		nom.load(ctx);
		g.arrayLoad(type);
		return type;
	}
}
