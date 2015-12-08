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
 * Defines methods which allow to create a local variable
 */
public final class VarLocal implements Variable {
	private final int local;

	VarLocal(int local) {
		this.local = local;
	}

	@Override
	public Type type(Context ctx) {
		return ctx.getGeneratorAdapter().getLocalType(local);
	}

	@Override
	public Type load(Context ctx) {
		ctx.getGeneratorAdapter().loadLocal(local);
		return type(ctx);
	}

	@Override
	public Object beginStore(Context ctx) {
		return null;
	}

	@Override
	public void store(Context ctx, Object storeContext, Type type) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		g.storeLocal(local);
	}

	public void store(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		g.storeLocal(local);
	}

	public void storeLocal(GeneratorAdapter g) {
		g.storeLocal(local);
	}

	public int getLocal() {
		return local;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		VarLocal varLocal = (VarLocal) o;

		return local == varLocal.local;
	}

	@Override
	public int hashCode() {
		return local;
	}
}
