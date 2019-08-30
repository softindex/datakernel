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

import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.Utils.*;
import static org.objectweb.asm.Type.getType;

public abstract class AbstractExpressionIteratorForEach implements Expression {
	protected final Expression collection;
	protected final Class<?> type;
	protected final Function<Expression, Expression> forEach;

	protected AbstractExpressionIteratorForEach(Expression collection, Class<?> type, Function<Expression, Expression> forEach) {
		this.collection = collection;
		this.type = type;
		this.forEach = forEach;
	}

	protected abstract Expression getValue(VarLocal varIt);

	@Override
	public final Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		Type collectionType = collection.load(ctx);
		if (collectionType.getSort() == Type.ARRAY) {
			return arrayForEach(ctx, g, labelLoop, labelExit);
		}

		VarLocal varIter = newLocal(ctx, getType(Iterator.class));

		Class<?> t = tryGetJavaType(collectionType);
		if (t.isInstance(Iterator.class) || t == Iterator.class) {
			// do nothing
		} else {
			invokeVirtualOrInterface(g, getJavaType(ctx.getClassLoader(), collectionType),
					new org.objectweb.asm.commons.Method("iterator", getType(Iterator.class), new Type[]{}));
		}
		varIter.store(ctx);

		g.mark(labelLoop);

		call(varIter, "hasNext").load(ctx);
		g.push(false);
		g.ifCmp(Type.BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		cast(call(varIter, "next"), type).load(ctx);
		VarLocal it = newLocal(ctx, getType(type));
		it.store(ctx);

		forEach.apply(getValue(it)).load(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;

	}

	public Type arrayForEach(Context ctx, GeneratorAdapter g, Label labelLoop, Label labelExit) {
		VarLocal len = newLocal(ctx, Type.INT_TYPE);
		g.arrayLength();
		len.store(ctx);

		g.push(0);
		VarLocal varPosition = newLocal(ctx, Type.INT_TYPE);
		varPosition.store(ctx);

		g.mark(labelLoop);

		varPosition.load(ctx);
		len.load(ctx);

		g.ifCmp(Type.INT_TYPE, GeneratorAdapter.GE, labelExit);

		collection.load(ctx);
		varPosition.load(ctx);
		g.arrayLoad(getType(type));

		VarLocal it = newLocal(ctx, getType(type));
		it.store(ctx);

		forEach.apply(getValue(it)).load(ctx);

		varPosition.load(ctx);
		g.push(1);
		g.math(GeneratorAdapter.ADD, Type.INT_TYPE);
		varPosition.store(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);

		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AbstractExpressionIteratorForEach that = (AbstractExpressionIteratorForEach) o;

		if (!collection.equals(that.collection)) return false;
		if (!forEach.equals(that.forEach)) return false;
		return type.equals(that.type);
	}

	@Override
	public int hashCode() {
		int result = collection.hashCode();
		result = 31 * result + forEach.hashCode();
		result = 31 * result + type.hashCode();
		return result;
	}
}
