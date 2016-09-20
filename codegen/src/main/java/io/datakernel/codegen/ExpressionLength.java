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

import static io.datakernel.codegen.Expressions.call;

final class ExpressionLength implements Expression {
	private final Expression field;

	ExpressionLength(Expression field) {
		this.field = field;
	}

	@Override
	public Type type(Context ctx) {
		return Type.INT_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		if (field.type(ctx).getSort() == Type.ARRAY) {
			field.load(ctx);
			g.arrayLength();
		} else if (field.type(ctx).getSort() == Type.OBJECT) {
			call(field, "size").load(ctx);
		}
		return Type.INT_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionLength that = (ExpressionLength) o;

		return !(field != null ? !field.equals(that.field) : that.field != null);

	}

	@Override
	public int hashCode() {
		return field != null ? field.hashCode() : 0;
	}
}
