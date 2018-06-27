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

import static io.datakernel.codegen.Expressions.call;
import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.codegen.Utils.newLocal;
import static io.datakernel.codegen.Utils.tryGetJavaType;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

final class ExpressionIteratorForEach implements Expression {
	private final Expression collection;
	private final Class<?> type;
	private final Expression forEach;

	ExpressionIteratorForEach(Expression collection, Class<?> type, Expression forEach) {
		this.collection = checkNotNull(collection);
		this.type = checkNotNull(type);
		this.forEach = checkNotNull(forEach);
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

		if (collection.type(ctx).getSort() == Type.ARRAY) {
			VarLocal len = newLocal(ctx, Type.INT_TYPE);
			collection.load(ctx);
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

			VarLocal varIt = newLocal(ctx, getType(type));
			varIt.store(ctx);

			ctx.addParameter("it", varIt);
			forEach.load(ctx);

			varPosition.load(ctx);
			g.push(1);
			g.math(GeneratorAdapter.ADD, Type.INT_TYPE);
			varPosition.store(ctx);

			g.goTo(labelLoop);
			g.mark(labelExit);

			return Type.VOID_TYPE;
		}

		VarLocal varIter = newLocal(ctx, getType(Iterator.class));

		Class<?> t = tryGetJavaType(collection.type(ctx));
		if (t.isInstance(Iterator.class) || t == Iterator.class) {
			collection.load(ctx);
			varIter.store(ctx);
		} else {
			call(collection, "iterator").load(ctx);
			varIter.store(ctx);
		}

		g.mark(labelLoop);

		call(varIter, "hasNext").load(ctx);
		g.push(false);
		g.ifCmp(Type.BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		cast(call(varIter, "next"), type).load(ctx);
		VarLocal varIt = newLocal(ctx, getType(type));
		varIt.store(ctx);

		ctx.addParameter("it", varIt);
		forEach.load(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionIteratorForEach that = (ExpressionIteratorForEach) o;

		if (!collection.equals(that.collection)) return false;
		if (!type.equals(that.type)) return false;
		if (!forEach.equals(that.forEach)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = collection.hashCode();
		result = 31 * result + type.hashCode();
		result = 31 * result + forEach.hashCode();
		return result;
	}
}
