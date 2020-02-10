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
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		g.loadLocal(local);
		return g.getLocalType(local);
	}

	@Nullable
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

	public int getLocal() {
		return local;
	}
}
