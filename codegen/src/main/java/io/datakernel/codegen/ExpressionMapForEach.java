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

import java.util.Iterator;
import java.util.Map;

import static io.datakernel.codegen.Expressions.call;
import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.codegen.Utils.newLocal;

final class ExpressionMapForEach implements Expression {
	private final Expression field;
	private final ForVar forKey;
	private final ForVar forValue;

	ExpressionMapForEach(Expression field, ForVar forKey, ForVar forValue) {
		this.field = field;
		this.forKey = forKey;
		this.forValue = forValue;
	}

	@Override
	public Type type(Context ctx) {
		return Type.VOID_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		call(call(field, "entrySet"), "iterator").load(ctx);
		VarLocal varIter = newLocal(ctx, Type.getType(Iterator.class));
		varIter.store(ctx);

		g.mark(labelLoop);

		call(varIter, "hasNext").load(ctx);
		g.push(false);
		g.ifCmp(Type.BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		cast(call(varIter, "next"), Map.Entry.class).load(ctx);
		VarLocal varEntry = newLocal(ctx, Type.getType(Map.Entry.class));

		varEntry.store(ctx);

		forKey.forVar(call(varEntry, "getKey")).load(ctx);
		forValue.forVar(call(varEntry, "getValue")).load(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionMapForEach that = (ExpressionMapForEach) o;

		return !(field != null ? !field.equals(that.field) : that.field != null);

	}

	@Override
	public int hashCode() {
		return field != null ? field.hashCode() : 0;
	}
}
