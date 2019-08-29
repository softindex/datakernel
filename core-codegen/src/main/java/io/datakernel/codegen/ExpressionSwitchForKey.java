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

import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

public final class ExpressionSwitchForKey implements Expression {
	private final Expression key;
	@Nullable
	private Expression defaultExp;
	private final List<Expression> listKey = new ArrayList<>();
	private final List<Expression> listValue = new ArrayList<>();

	ExpressionSwitchForKey(Expression key, @Nullable Expression defaultExp,
			List<Expression> listKey, List<Expression> listValue) {
		this.key = checkNotNull(key);
		this.defaultExp = defaultExp;
		this.listKey.addAll(listKey);
		this.listValue.addAll(listValue);
	}

	ExpressionSwitchForKey(Expression key) {
		this.key = checkNotNull(key);
	}

	public static ExpressionSwitchForKey create(Expression key) {
		return new ExpressionSwitchForKey(key);
	}

	public ExpressionSwitchForKey with(Expression key, Expression value) {
		listKey.add(key);
		listValue.add(value);
		return this;
	}

	public ExpressionSwitchForKey withDefault(Expression defaultExp) {
		this.defaultExp = defaultExp;
		return this;
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

		boolean keyPrimitiveType = Utils.isPrimitiveType(key.type(ctx));
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
			Variable sb = new ExpressionLet(constructor(StringBuilder.class));
			call(sb, "append", value("Key '")).load(ctx);
			call(sb, "append", keyPrimitiveType ? varKey : call(key, "toString")).load(ctx);
			call(sb, "append", value("' not in keyList: [")).load(ctx);
			Iterator<Expression> iterator = listKey.iterator();
			while (iterator.hasNext()) {
				Expression expression = iterator.next();
				boolean primitiveType = Utils.isPrimitiveType(expression.type(ctx));
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

		if (!key.equals(that.key)) return false;
		if (!Objects.equals(defaultExp, that.defaultExp)) return false;
		if (!listKey.equals(that.listKey)) return false;
		return listValue.equals(that.listValue);
	}

	@Override
	public int hashCode() {
		int result = key.hashCode();
		result = 31 * result + (defaultExp == null ? 0 : defaultExp.hashCode());
		result = 31 * result + listKey.hashCode();
		result = 31 * result + listValue.hashCode();
		return result;
	}
}
