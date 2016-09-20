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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.Utils.newLocal;
import static org.objectweb.asm.Type.getType;

final class ExpressionSwitchForKey implements Expression {
	private final Expression key;
	private final Expression defaultExp;
	private final List<Expression> listKey = new ArrayList<>();
	private final List<Expression> listValue = new ArrayList<>();

	ExpressionSwitchForKey(Expression key, List<Expression> listKey, List<Expression> listValue) {
		this.key = key;
		this.defaultExp = null;
		this.listKey.addAll(listKey);
		this.listValue.addAll(listValue);
	}

	ExpressionSwitchForKey(Expression key, Expression defalultExp, List<Expression> listKey, List<Expression> listValue) {
		this.key = key;
		this.defaultExp = defalultExp;
		this.listKey.addAll(listKey);
		this.listValue.addAll(listValue);
	}

	@Override
	public Type type(Context ctx) {
		if (listValue.size() != 0) {
			return listValue.get(0).type(ctx);
		} else {
			return getType(Object.class);
		}
	}

	@Override
	public Type load(Context ctx) {
		VarLocal varKey = newLocal(ctx, key.type(ctx));
		key.load(ctx);
		varKey.store(ctx);

		Label labelExit = new Label();
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		final boolean keyPrimitiveType = Utils.isPrimitiveType(key.type(ctx));
		for (int i = 0; i < listKey.size(); i++) {
			Label labelNext = new Label();
			if (keyPrimitiveType) {
				listKey.get(i).load(ctx);
				varKey.load(ctx);
				g.ifCmp(key.type(ctx), GeneratorAdapter.NE, labelNext);
			} else {
				call(listKey.get(i), "equals", varKey).load(ctx);
				g.push(true);
				g.ifCmp(Type.BOOLEAN_TYPE, GeneratorAdapter.NE, labelNext);
			}

			listValue.get(i).load(ctx);
			g.goTo(labelExit);

			g.mark(labelNext);
		}

		if (defaultExp != null) {
			defaultExp.load(ctx);
		} else {
			final Variable sb = let(constructor(StringBuilder.class));
			call(sb, "append", value("Key '")).load(ctx);
			call(sb, "append", keyPrimitiveType ? varKey : call(key, "toString")).load(ctx);
			call(sb, "append", value("' not in keyList: [")).load(ctx);
			final Iterator<Expression> iterator = listKey.iterator();
			while (iterator.hasNext()) {
				final Expression expression = iterator.next();
				final boolean primitiveType = Utils.isPrimitiveType(expression.type(ctx));
				call(sb, "append", primitiveType ? expression : call(expression, "toString")).load(ctx);
				if (iterator.hasNext()) {
					call(sb, "append", value(", ")).load(ctx);
				}
			}
			call(sb, "append", value("]")).load(ctx);

			constructor(IllegalArgumentException.class, call(sb, "toString")).load(ctx);
			g.throwException();
		}
		g.mark(labelExit);

		return type(ctx);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionSwitchForKey that = (ExpressionSwitchForKey) o;

		if (key != null ? !key.equals(that.key) : that.key != null) return false;
		if (listKey != null ? !listKey.equals(that.listKey) : that.listKey != null) return false;
		return !(listValue != null ? !listValue.equals(that.listValue) : that.listValue != null);

	}

	@Override
	public int hashCode() {
		int result = key != null ? key.hashCode() : 0;
		result = 31 * result + (listKey != null ? listKey.hashCode() : 0);
		result = 31 * result + (listValue != null ? listValue.hashCode() : 0);
		return result;
	}
}
