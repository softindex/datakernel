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

/**
 * Defines methods for using a variable which was created with method 'let' before
 */
public final class FunctionDefVar implements FunctionDef {
	private final String name;

	public FunctionDefVar(String name) {
		this.name = name;
	}

	@Override
	public Type type(Context ctx) {
		return ctx.getLocal(name).type(ctx);
	}

	@Override
	public Type load(Context ctx) {
		VarLocal var = ctx.getLocal(name);
		var.load(ctx);
		return var.type(ctx);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefVar that = (FunctionDefVar) o;

		return name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}
}
