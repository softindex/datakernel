/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import java.util.function.Function;

import static io.datakernel.codegen.Expressions.*;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.getType;

public abstract class AbstractExpressionMapForEach implements Expression {
	protected final Expression collection;
	protected final Function<Expression, Expression> forKey;
	protected final Function<Expression, Expression> forValue;
	protected final Class<?> entryType;

	protected AbstractExpressionMapForEach(Expression collection, Function<Expression, Expression> forKey, Function<Expression, Expression> forValue, Class<?> entryType) {
		this.collection = collection;
		this.forKey = forKey;
		this.forValue = forValue;
		this.entryType = entryType;
	}

	protected abstract Expression getEntries();

	protected abstract Expression getKey(VarLocal entry);

	protected abstract Expression getValue(VarLocal entry);

	@Override
	public final Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		Expression it = call(getEntries(), "iterator");
		it.load(ctx);
		VarLocal iterator = newLocal(ctx, getType(Iterator.class));
		iterator.store(ctx);

		g.mark(labelLoop);

		call(iterator, "hasNext").load(ctx);
		g.push(false);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		Expression varEntry = cast(call(iterator, "next"), entryType);
		Type varEntryType = varEntry.load(ctx);

		VarLocal entry = newLocal(ctx, varEntryType);
		entry.store(ctx);

		forKey.apply(getKey(entry)).load(ctx);
		forValue.apply(getValue(entry)).load(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AbstractExpressionMapForEach that = (AbstractExpressionMapForEach) o;

		if (!collection.equals(that.collection)) return false;
		if (!forKey.equals(that.forKey)) return false;
		if (!forValue.equals(that.forValue)) return false;
		return entryType.equals(that.entryType);
	}

	@Override
	public int hashCode() {
		int result = collection.hashCode();
		result = 31 * result + forKey.hashCode();
		result = 31 * result + forValue.hashCode();
		result = 31 * result + entryType.hashCode();
		return result;
	}
}
