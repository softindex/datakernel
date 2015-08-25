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

public class FunctionDefForEach implements FunctionDef {
	private final FunctionDef field;
	private final FunctionDef start;
	private final FunctionDef len;
	private final ForVar forVar;

	FunctionDefForEach(FunctionDef field, ForVar forVar) {
		this.field = field;
		this.forVar = forVar;
		this.start = value(0);
		this.len = length(field);
	}

	FunctionDefForEach(FunctionDef field, FunctionDef start, FunctionDef finish, ForVar forVar) {
		this.field = field;
		this.start = start;
		this.len = finish;
		this.forVar = forVar;
	}

	@Override
	public Type type(Context ctx) {
		return Type.VOID_TYPE;
	}

	@Override
	public Type load(final Context ctx) {
		functionFor(start, len, new ForVar() {
			@Override
			public FunctionDef forVar(FunctionDef eachNumber) {
				return FunctionDefForEach.this.forVar.forVar(get(field, eachNumber));
			}
		}).load(ctx);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefForEach forEach = (FunctionDefForEach) o;

		if (field != null ? !field.equals(forEach.field) : forEach.field != null) return false;
		if (start != null ? !start.equals(forEach.start) : forEach.start != null) return false;
		if (len != null ? !len.equals(forEach.len) : forEach.len != null) return false;
		return !(forVar != null ? !forVar.equals(forEach.forVar) : forEach.forVar != null);

	}

	@Override
	public int hashCode() {
		int result = field != null ? field.hashCode() : 0;
		result = 31 * result + (start != null ? start.hashCode() : 0);
		result = 31 * result + (len != null ? len.hashCode() : 0);
		result = 31 * result + (forVar != null ? forVar.hashCode() : 0);
		return result;
	}
}