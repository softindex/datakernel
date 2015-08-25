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

import java.util.ArrayList;
import java.util.List;

import static org.objectweb.asm.Type.VOID_TYPE;

/**
 * Defines methods which allow to use several methods one after the other
 */
@SuppressWarnings("PointlessArithmeticExpression")
public final class FunctionDefSequence implements FunctionDef {
	private final List<FunctionDef> functionDefs = new ArrayList<>();

	public FunctionDefSequence(List<FunctionDef> functionDefs) {
		this.functionDefs.addAll(functionDefs);
	}

	public FunctionDefSequence add(FunctionDef functionDef) {
		functionDefs.add(functionDef);
		return this;
	}

	@Override
	public Type type(Context ctx) {
		FunctionDef functionDef = getLast(functionDefs);
		return functionDef.type(ctx);
	}

	private FunctionDef getLast(List<FunctionDef> list) {
		return list.get(list.size() - 1);
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type type = VOID_TYPE;
		for (int i = 0; i < functionDefs.size(); i++) {
			FunctionDef functionDef = functionDefs.get(i);
			type = functionDef.load(ctx);
			if (i != functionDefs.size() - 1) {
				if (type.getSize() == 1)
					g.pop();
				if (type.getSize() == 2)
					g.pop2();
			}
		}

		return type;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefSequence that = (FunctionDefSequence) o;

		return functionDefs.equals(that.functionDefs);
	}

	@Override
	public int hashCode() {
		return functionDefs.hashCode();
	}
}
