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
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.getType;

final class ForEachHppcMap implements Expression {
	private final Class<?> iteratorType;
	private final Expression collection;
	private final Expression forKey;
	private final Expression forValue;

	ForEachHppcMap(Class<?> iteratorType, Expression collection, Expression forKey, Expression forValue) {
		this.iteratorType = checkNotNull(iteratorType);
		this.collection = checkNotNull(collection);
		this.forKey = checkNotNull(forKey);
		this.forValue = checkNotNull(forValue);
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

		VarLocal iterator = newLocal(ctx, getType(Iterator.class));
		call(collection, "iterator").load(ctx);
		iterator.store(ctx);

		g.mark(labelLoop);

		call(iterator, "hasNext").load(ctx);
		g.push(true);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.NE, labelExit);

		VarLocal item = newLocal(ctx, getType(iteratorType));

		cast(call(iterator, "next"), iteratorType).load(ctx);
		item.store(ctx);

		ctx.addParameter("key", field(item, "key"));
		forKey.load(ctx);

		ctx.addParameter("value", field(item, "value"));
		forValue.load(ctx);

		g.goTo(labelLoop);

		g.mark(labelExit);

		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ForEachHppcMap that = (ForEachHppcMap) o;

		if (!iteratorType.equals(that.iteratorType)) return false;
		if (!collection.equals(that.collection)) return false;
		if (!forKey.equals(that.forKey)) return false;
		if (!forValue.equals(that.forValue)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = iteratorType.hashCode();
		result = 31 * result + collection.hashCode();
		result = 31 * result + forKey.hashCode();
		result = 31 * result + forValue.hashCode();
		return result;
	}

}
