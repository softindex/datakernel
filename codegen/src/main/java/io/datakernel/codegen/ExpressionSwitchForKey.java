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
import java.util.List;

import static io.datakernel.codegen.Expressions.call;
import static io.datakernel.codegen.Utils.newLocal;
import static org.objectweb.asm.Type.getType;

public class ExpressionSwitchForKey implements Expression {
	private final Expression key;
	private final List<Expression> listKey = new ArrayList<>();
	private final List<Expression> listValue = new ArrayList<>();

	ExpressionSwitchForKey(Expression key, List<Expression> listKey, List<Expression> listValue) {
		this.key = key;
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

		for (int i = 0; i < listKey.size(); i++) {
			Label labelNext = new Label();
			if (Utils.isPrimitiveType(key.type(ctx))) {
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

		g.throwException(getType(IllegalArgumentException.class), "");
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
