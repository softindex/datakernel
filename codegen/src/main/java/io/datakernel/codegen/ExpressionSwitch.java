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

import java.util.List;
import java.util.Objects;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.Type.getType;

final class ExpressionSwitch implements Expression {
	private final Expression index;
	private final List<Expression> list;
	private final Expression defaultExp;

	ExpressionSwitch(Expression index, Expression defaultExp, List<Expression> list) {
		this.index = checkNotNull(index);
		this.list = checkNotNull(list);
		this.defaultExp = defaultExp;
	}

	@Override
	public Type type(Context ctx) {
		if (list.size() != 0) {
			return list.get(0).type(ctx);
		} else {
			return getType(Object.class);
		}
	}

	@Override
	public Type load(Context ctx) {
		VarLocal varReadedSubClass = newLocal(ctx, index.type(ctx));
		index.load(ctx);
		varReadedSubClass.store(ctx);

		Label labelExit = new Label();
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		for (int i = 0; i < list.size(); i++) {
			Label labelNext = new Label();

			g.push(i);
			varReadedSubClass.load(ctx);
			g.ifCmp(INT_TYPE, GeneratorAdapter.NE, labelNext);

			list.get(i).load(ctx);
			g.goTo(labelExit);

			g.mark(labelNext);
		}

		if (defaultExp != null) {
			defaultExp.load(ctx);
		} else {
			Variable sb = let(constructor(StringBuilder.class));
			call(sb, "append", value("Key '")).load(ctx);
			call(sb, "append", cast(varReadedSubClass, getType(int.class))).load(ctx);
			call(sb, "append", value(String.format("' not in range [0-%d)", list.size()))).load(ctx);
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

		ExpressionSwitch that = (ExpressionSwitch) o;

		if (!index.equals(that.index)) return false;
		if (!list.equals(that.list)) return false;
		return Objects.equals(defaultExp, that.defaultExp);

	}

	@Override
	public int hashCode() {
		int result = index.hashCode();
		result = 31 * result + list.hashCode();
		result = 31 * result + (defaultExp == null ? 0 : list.hashCode());
		return result;
	}
}
