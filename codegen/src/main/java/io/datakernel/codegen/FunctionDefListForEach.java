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

import static io.datakernel.codegen.FunctionDefs.*;

public class FunctionDefListForEach implements FunctionDef {
	private final FunctionDef field;
	private final ForVar forVar;

	FunctionDefListForEach(FunctionDef field, ForVar forVar) {
		this.field = field;
		this.forVar = forVar;
	}

	@Override
	public Type type(Context ctx) {
		return Type.VOID_TYPE;
	}

	@Override
	public Type load(final Context ctx) {
		functionFor(value(0), length(field), new ForVar() {
			@Override
			public FunctionDef forVar(FunctionDef pos) {
				return FunctionDefListForEach.this.forVar.forVar(call(field, "get", pos));
			}
		}).load(ctx);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefListForEach that = (FunctionDefListForEach) o;

		if (field != null ? !field.equals(that.field) : that.field != null) return false;
		return !(forVar != null ? !forVar.equals(that.forVar) : that.forVar != null);

	}

	@Override
	public int hashCode() {
		int result = field != null ? field.hashCode() : 0;
		result = 31 * result + (forVar != null ? forVar.hashCode() : 0);
		return result;
	}
}
