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

import io.datakernel.codegen.utils.Primitives;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

/**
 * Defines methods to create a constant value
 */
final class ExpressionConstant implements Expression {

	private final Object value;
	private String staticConstantField;

	ExpressionConstant(Object value) {
		checkNotNull(value);
		this.value = value;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public Type type(Context ctx) {
		if (value instanceof String) {
			return getType(String.class);
		} else if (value instanceof Type) {
			return (Type) value;
		} else {
			return getType(Primitives.unwrap(value.getClass()));
		}
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type type = type(ctx);
		if (value instanceof Byte) {
			g.push((Byte) value);
		} else if (value instanceof Short) {
			g.push((Short) value);
		} else if (value instanceof Integer) {
			g.push((Integer) value);
		} else if (value instanceof Long) {
			g.push((Long) value);
		} else if (value instanceof Float) {
			g.push((Float) value);
		} else if (value instanceof Double) {
			g.push((Double) value);
		} else if (value instanceof Boolean) {
			g.push((Boolean) value);
		} else if (value instanceof Character) {
			g.push((Character) value);
		} else if (value instanceof String) {
			g.push((String) value);
		} else if (value instanceof Type) {
			g.push((Type) value);
		} else if (value instanceof Enum) {
			g.getStatic(type, ((Enum) value).name(), type);
		} else {
			if (staticConstantField == null) {
				staticConstantField = "$STATIC_CONSTANT_" + (ctx.getStaticConstants().size() + 1);
				ctx.addStaticConstant(staticConstantField, value);
			}
			g.getStatic(ctx.getThisType(), staticConstantField, getType(value.getClass()));
		}
		return type;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionConstant that = (ExpressionConstant) o;

		if (value != null ? !value.equals(that.value) : that.value != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return value != null ? value.hashCode() : 0;
	}
}
