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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.codegen.Utils.*;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.objectweb.asm.Type.getType;

final class ExpressionCallStatic implements Expression {
	private final Class<?> owner;
	private final String name;
	private final List<Expression> arguments;

	ExpressionCallStatic(Class<?> owner, String name, List<Expression> arguments) {
		this.owner = checkNotNull(owner);
		this.name = checkNotNull(name);
		this.arguments = checkNotNull(arguments);
	}

	@Override
	public Type type(Context ctx) {
		List<Class<?>> argumentClasses = new ArrayList<>();
		for (Expression argument : arguments) {
			argumentClasses.add(getJavaType(ctx.getClassLoader(), argument.type(ctx)));
		}

		Class<?>[] arguments = argumentClasses.toArray(new Class<?>[]{});

		Type returnType;
		try {
			Method method = owner.getMethod(name, arguments);
			Class<?> returnClass = method.getReturnType();
			returnType = getType(returnClass);
		} catch (NoSuchMethodException ignored) {
			throw new RuntimeException(format("No static method %s.%s(%s). %s",
					owner.getName(),
					name,
					(!argumentClasses.isEmpty() ? argsToString(argumentClasses) : ""),
					exceptionInGeneratedClass(ctx)));
		}

		return returnType;
	}

	@Override
	public Type load(Context ctx) {
		List<Class<?>> argumentClasses = new ArrayList<>();
		for (Expression argument : arguments) {
			argument.load(ctx);
			if (argument.type(ctx).equals(getType(Object[].class))) {
				argumentClasses.add(Object[].class);
			} else {
				argumentClasses.add(getJavaType(ctx.getClassLoader(), argument.type(ctx)));
			}
		}

		Class<?>[] arguments = argumentClasses.toArray(new Class<?>[]{});
		Type returnType;
		Method method;
		try {
			Class<?> ownerJavaType = getJavaType(ctx.getClassLoader(), Type.getType(owner));
			method = ownerJavaType.getMethod(name, arguments);
			Class<?> returnClass = method.getReturnType();
			returnType = getType(returnClass);
		} catch (NoSuchMethodException ignored) {
			throw new RuntimeException(format("No static method %s.%s(%s). %s",
					owner.getName(),
					name,
					(!argumentClasses.isEmpty() ? argsToString(argumentClasses) : ""),
					exceptionInGeneratedClass(ctx)));

		}
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		g.invokeStatic(Type.getType(owner), org.objectweb.asm.commons.Method.getMethod(method));
		return returnType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionCallStatic that = (ExpressionCallStatic) o;

		if (!owner.equals(that.owner)) return false;
		if (!name.equals(that.name)) return false;
		if (!arguments.equals(that.arguments)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = owner.hashCode();
		result = 31 * result + name.hashCode();
		result = 31 * result + arguments.hashCode();
		return result;
	}
}
