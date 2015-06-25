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

import static io.datakernel.codegen.Utils.newLocal;

/**
 * Defines methods which allow to create variables
 */
public final class FunctionDefLet implements FunctionDef {
	private final FunctionDef functionDef;
	private final String name;

	public FunctionDefLet(FunctionDef functionDef, String name) {
		this.functionDef = functionDef;
		this.name = name;
	}

	@Override
	public Type type(Context ctx) {
		return functionDef.type(ctx);
	}

	@Override
	public Type load(Context ctx) {
		VarLocal var = ctx.hasLocal(name) ? ctx.getLocal(name) : null;
		if (var == null) {
			Type type = functionDef.load(ctx);
			var = newLocal(ctx, type);
			var.store(ctx);
			ctx.putLocal(name, var);
		}
		var.load(ctx);
		return var.type(ctx);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefLet that = (FunctionDefLet) o;

		return (functionDef.equals(that.functionDef)) && (name.equals(that.name));
	}

	@Override
	public int hashCode() {
		int result = functionDef.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}
}
