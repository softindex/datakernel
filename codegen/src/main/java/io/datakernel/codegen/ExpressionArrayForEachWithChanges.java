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

public class ExpressionArrayForEachWithChanges implements Expression {
	private final Expression field;
	private final ForEachWithChanges forEachWithChanges;
	private final Expression len;
	private final Expression start;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionArrayForEachWithChanges that = (ExpressionArrayForEachWithChanges) o;

		if (field != null ? !field.equals(that.field) : that.field != null) return false;
		if (forEachWithChanges != null ? !forEachWithChanges.equals(that.forEachWithChanges) : that.forEachWithChanges != null)
			return false;
		if (len != null ? !len.equals(that.len) : that.len != null) return false;
		return !(start != null ? !start.equals(that.start) : that.start != null);

	}

	@Override
	public int hashCode() {
		int result = field != null ? field.hashCode() : 0;
		result = 31 * result + (forEachWithChanges != null ? forEachWithChanges.hashCode() : 0);
		result = 31 * result + (len != null ? len.hashCode() : 0);
		result = 31 * result + (start != null ? start.hashCode() : 0);
		return result;
	}

	ExpressionArrayForEachWithChanges(Expression field, ForEachWithChanges forEachWithChanges) {
		this.field = field;
		this.start = value(0);
		this.len = length(field);
		this.forEachWithChanges = forEachWithChanges;
	}

	ExpressionArrayForEachWithChanges(Expression field, Expression start, Expression length, ForEachWithChanges forEachWithChanges) {
		this.field = field;
		this.start = start;
		this.len = length;
		this.forEachWithChanges = forEachWithChanges;
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
				return setForArray(field, eachNumber, ExpressionArrayForEachWithChanges.this.forEachWithChanges.forEachWithChanges());
			}
		}).load(ctx);
		return Type.VOID_TYPE;
	}
}
