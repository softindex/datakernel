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

import static io.datakernel.codegen.Utils.loadAndCast;
import static org.objectweb.asm.Type.getType;

/**
 * Defines method in order to cast a function to a type
 */
public final class FunctionDefCast implements FunctionDef {
	public static final Type THIS_TYPE = getType(Object.class);

	private final FunctionDef functionDef;
	private final Type targetType;

	FunctionDefCast(FunctionDef functionDef, Type type) {
		this.functionDef = functionDef;
		this.targetType = type;
	}

	@Override
	public Type type(Context ctx) {
		return targetType == THIS_TYPE ? ctx.getThisType() : targetType;
	}

	@Override
	public Type load(Context ctx) {
		Type targetType = type(ctx);
		loadAndCast(ctx, functionDef, targetType);
		return targetType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefCast that = (FunctionDefCast) o;

		if (functionDef != null ? !functionDef.equals(that.functionDef) : that.functionDef != null) return false;
		if (targetType != null ? !targetType.equals(that.targetType) : that.targetType != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = functionDef != null ? functionDef.hashCode() : 0;
		result = 31 * result + (targetType != null ? targetType.hashCode() : 0);
		return result;
	}
}
