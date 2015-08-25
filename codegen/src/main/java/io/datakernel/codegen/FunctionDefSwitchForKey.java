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

import java.util.List;

import static io.datakernel.codegen.FunctionDefs.call;
import static io.datakernel.codegen.Utils.newLocal;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.getType;

public class FunctionDefSwitchForKey implements FunctionDef {
	private final FunctionDef key;
	private final Type returnType;
	private final List<FunctionDef> listKey;
	private final List<FunctionDef> listValue;

	FunctionDefSwitchForKey(FunctionDef key, Type returnType, List<FunctionDef> listKey, List<FunctionDef> listValue) {
		this.key = key;
		this.returnType = returnType;
		this.listKey = listKey;
		this.listValue = listValue;
	}

	@Override
	public Type type(Context ctx) {
		return returnType;
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
				g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.NE, labelNext);
			}

			listValue.get(i).load(ctx);
			g.goTo(labelExit);

			g.mark(labelNext);
		}

		g.throwException(getType(IllegalArgumentException.class), "");
		g.mark(labelExit);

		return returnType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefSwitchForKey that = (FunctionDefSwitchForKey) o;

		if (key != null ? !key.equals(that.key) : that.key != null) return false;
		if (returnType != null ? !returnType.equals(that.returnType) : that.returnType != null) return false;
		if (listKey != null ? !listKey.equals(that.listKey) : that.listKey != null) return false;
		return !(listValue != null ? !listValue.equals(that.listValue) : that.listValue != null);

	}

	@Override
	public int hashCode() {
		int result = key != null ? key.hashCode() : 0;
		result = 31 * result + (returnType != null ? returnType.hashCode() : 0);
		result = 31 * result + (listKey != null ? listKey.hashCode() : 0);
		result = 31 * result + (listValue != null ? listValue.hashCode() : 0);
		return result;
	}
}
