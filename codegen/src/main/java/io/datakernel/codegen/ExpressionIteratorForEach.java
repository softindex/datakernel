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
import static io.datakernel.codegen.Utils.tryGetJavaType;
import static org.objectweb.asm.Type.getType;

public class ExpressionIteratorForEach implements Expression {
	private final Expression collection;
	private final ForVar forCollection;
	private final Class<?> type;

	public ExpressionIteratorForEach(Expression collection, ForVar forCollection) {
		this.collection = collection;
		this.forCollection = forCollection;
		this.type = Object.class;
	}

	public ExpressionIteratorForEach(Expression collection, Class<?> type, ForVar forCollection) {
		this.collection = collection;
		this.type = type;
		this.forCollection = forCollection;
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
			expressionFor(length(collection), new ForVar() {
				@Override
				public Expression forVar(Expression it) {
					return forCollection.forVar(getArrayItem(collection, it));
				}
			}).load(ctx);
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
		VarLocal varKey = newLocal(ctx, getType(type));
		varKey.store(ctx);

		forCollection.forVar(varKey).load(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionIteratorForEach that = (ExpressionIteratorForEach) o;

		if (collection != null ? !collection.equals(that.collection) : that.collection != null) return false;
		return !(type != null ? !type.equals(that.type) : that.type != null);

	}

	@Override
	public int hashCode() {
		int result = collection != null ? collection.hashCode() : 0;
		result = 31 * result + (type != null ? type.hashCode() : 0);
		return result;
	}
}
