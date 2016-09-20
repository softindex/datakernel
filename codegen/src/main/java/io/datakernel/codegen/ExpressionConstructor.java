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

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.Utils.*;
import static java.lang.String.format;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods for using constructors from other classes
 */
final class ExpressionConstructor implements Expression {
	private final Class<?> type;
	private final List<Expression> fields;

	ExpressionConstructor(Class<?> type, Expression... fields) {
		this.type = type;
		this.fields = Arrays.asList(fields);
	}

	ExpressionConstructor(Class<?> type, List<Expression> fields) {
		this.type = type;
		this.fields = fields;
	}

	@Override
	public Type type(Context ctx) {
		return getType(type);
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Class<?>[] fieldTypes = new Class<?>[this.fields.size()];
		Expression[] fieldVars = new Expression[this.fields.size()];
		for (int i = 0; i < this.fields.size(); i++) {
			Expression field = this.fields.get(i);
			Type fieldType = field.type(ctx);
			fieldTypes[i] = getJavaType(ctx.getClassLoader(), fieldType);
			fieldVars[i] = field;
		}
		try {
			Constructor<?> constructor = type.getConstructor(fieldTypes);
			g.newInstance(getType(type));
			g.dup();
			for (Expression fieldVar : fieldVars) {
				fieldVar.load(ctx);
			}
			g.invokeConstructor(getType(type), getMethod(constructor));
			return getType(type);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(format("No constructor %s.<init>(%s). %s",
					type.getName(),
					(fieldTypes.length != 0 ? argsToString(fieldTypes) : ""),
					exceptionInGeneratedClass(ctx)));
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionConstructor that = (ExpressionConstructor) o;

		if (fields != null ? !fields.equals(that.fields) : that.fields != null) return false;
		if (type != null ? !type.equals(that.type) : that.type != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = type != null ? type.hashCode() : 0;
		result = 31 * result + (fields != null ? fields.hashCode() : 0);
		return result;
	}
}
