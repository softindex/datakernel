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

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

/**
 * Defines methods for caching a function in context
 */
public final class FunctionDefCache implements FunctionDef {
	private final FunctionDef functionDef;

	protected FunctionDefCache(FunctionDef functionDef) {
		this.functionDef = functionDef;
	}

	@Override
	public Type type(Context ctx) {
		return functionDef.type(ctx);
	}

	@Override
	public final Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Integer var = ctx.getCache(functionDef);
		if (var != null) {
			g.loadLocal(var);
			return g.getLocalType(var);
		} else {
			Type type = functionDef.load(ctx);
			if (type != Type.VOID_TYPE) {
				var = g.newLocal(type);
				g.storeLocal(var);
				ctx.putCache(functionDef, var);
				g.loadLocal(var);
			}
			return type;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefCache that = (FunctionDefCache) o;

		return functionDef.equals(that.functionDef);
	}

	@Override
	public int hashCode() {
		return functionDef.hashCode();
	}
}
