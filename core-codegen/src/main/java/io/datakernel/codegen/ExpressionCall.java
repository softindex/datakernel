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
import static java.lang.String.format;
import static org.objectweb.asm.Type.getType;

/**
 * Defines methods for using static methods from other classes
 */
final class ExpressionCall implements Expression {
	private final Expression owner;
	private final String methodName;
	private final List<Expression> arguments;

	ExpressionCall(Expression owner, String methodName, List<Expression> arguments) {
		this.owner = owner;
		this.methodName = methodName;
		this.arguments = arguments;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type ownerType = owner.load(ctx);

		List<Class<?>> argumentClasses = new ArrayList<>();
		List<Type> argumentTypes = new ArrayList<>();
		for (Expression argument : arguments) {
			Type argumentType = argument.load(ctx);
			argumentTypes.add(argumentType);
			argumentClasses.add(getJavaType(ctx.getClassLoader(), argumentType));
		}

		try {
			if (!ctx.getThisType().equals(ownerType)) {
				Class<?> ownerJavaType = getJavaType(ctx.getClassLoader(), ownerType);
				Method method = ownerJavaType.getMethod(methodName, argumentClasses.toArray(new Class<?>[]{}));
				Type returnType = getType(method.getReturnType());
				invokeVirtualOrInterface(g, ownerJavaType, new org.objectweb.asm.commons.Method(methodName,returnType, argumentTypes.toArray(new Type[]{})));
				return returnType;
			}
			outer:
			for (org.objectweb.asm.commons.Method method : ctx.getMethods().keySet()) {
				if (!method.getName().equals(methodName) || method.getArgumentTypes().length != arguments.size()) {
					continue;
				}
				Type[] methodTypes = method.getArgumentTypes();
				for (int i = 0; i < arguments.size(); i++) {
					if (!methodTypes[i].equals(argumentTypes.get(i))) {
						continue outer;
					}
				}
				g.invokeVirtual(ownerType, method);
				return method.getReturnType();
			}
			throw new NoSuchMethodException("goto catch block");
		} catch (NoSuchMethodException ignored) {
			throw new RuntimeException(format("No method %s.%s(%s). %s",
					ownerType.getClassName(),
					methodName,
					(!argumentClasses.isEmpty() ? argsToString(argumentClasses) : ""),
					exceptionInGeneratedClass(ctx)));
		}
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ExpressionCall that = (ExpressionCall) o;

		if (!owner.equals(that.owner)) {
			return false;
		}
		if (!methodName.equals(that.methodName)) {
			return false;
		}
		if (!arguments.equals(that.arguments)) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = owner.hashCode();
		result = 31 * result + methodName.hashCode();
		result = 31 * result + arguments.hashCode();
		return result;
	}
}
