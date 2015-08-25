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

import static io.datakernel.codegen.FunctionDefs.call;
import static io.datakernel.codegen.FunctionDefs.cast;
import static io.datakernel.codegen.Utils.newLocal;

public class FunctionDefSetForEach implements FunctionDef {
	private final FunctionDef field;
	private final ForVar forKey;

	FunctionDefSetForEach(FunctionDef field, ForVar forKey) {
		this.field = field;
		this.forKey = forKey;
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

		call(field, "iterator").load(ctx);
		VarLocal varIter = newLocal(ctx, Type.getType(Iterator.class));
		varIter.store(ctx);

		g.mark(labelLoop);

		call(varIter, "hasNext").load(ctx);
		g.push(false);
		g.ifCmp(Type.BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		cast(call(varIter, "next"), Object.class).load(ctx);
		VarLocal varKey = newLocal(ctx, Type.getType(Object.class));
		varKey.store(ctx);

		forKey.forVar(varKey).load(ctx);

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefSetForEach that = (FunctionDefSetForEach) o;

		if (field != null ? !field.equals(that.field) : that.field != null) return false;
		return !(forKey != null ? !forKey.equals(that.forKey) : that.forKey != null);

	}

	@Override
	public int hashCode() {
		int result = field != null ? field.hashCode() : 0;
		result = 31 * result + (forKey != null ? forKey.hashCode() : 0);
		return result;
	}
}
