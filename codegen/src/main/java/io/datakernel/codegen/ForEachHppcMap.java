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

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.Utils.newLocal;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.getType;

final class ForEachHppcMap implements Expression {
	private final Class<?> iteratorType;
	private final Expression value;
	private final ForVar forKey;
	private final ForVar forValue;

	ForEachHppcMap(Class<?> iteratorType, Expression value, ForVar forKey, ForVar forValue) {
		this.iteratorType = iteratorType;
		this.value = value;
		this.forKey = forKey;
		this.forValue = forValue;
	}

	@Override
	public Type type(Context ctx) {
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ForEachHppcMap that = (ForEachHppcMap) o;

		if (iteratorType != null ? !iteratorType.equals(that.iteratorType) : that.iteratorType != null) return false;
		return !(value != null ? !value.equals(that.value) : that.value != null);

	}

	@Override
	public int hashCode() {
		int result = iteratorType != null ? iteratorType.hashCode() : 0;
		result = 31 * result + (value != null ? value.hashCode() : 0);
		return result;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		VarLocal iterator = newLocal(ctx, getType(Iterator.class));
		call(value, "iterator").load(ctx);
		iterator.store(ctx);

		g.mark(labelLoop);

		call(iterator, "hasNext").load(ctx);
		g.push(true);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.NE, labelExit);

		VarLocal item = newLocal(ctx, getType(iteratorType));

		cast(call(iterator, "next"), iteratorType).load(ctx);
		item.store(ctx);

		forKey.forVar(field(item, "key")).load(ctx);
		forValue.forVar(field(item, "value")).load(ctx);

		g.goTo(labelLoop);

		g.mark(labelExit);

		return Type.VOID_TYPE;
	}
}
