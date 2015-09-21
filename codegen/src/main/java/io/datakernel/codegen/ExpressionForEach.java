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

import static io.datakernel.codegen.Expressions.*;

public class ExpressionForEach implements Expression {
	private final Expression field;
	private final Expression start;
	private final Expression len;
	private final ForVar forVar;

	ExpressionForEach(Expression field, ForVar forVar) {
		this.field = field;
		this.forVar = forVar;
		this.start = value(0);
		this.len = length(field);
	}

	ExpressionForEach(Expression field, Expression start, Expression finish, ForVar forVar) {
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
		expressionFor(start, len, new ForVar() {
			@Override
			public Expression forVar(Expression eachNumber) {
				return ExpressionForEach.this.forVar.forVar(get(field, eachNumber));
			}
		}).load(ctx);
		return Type.VOID_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionForEach forEach = (ExpressionForEach) o;

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