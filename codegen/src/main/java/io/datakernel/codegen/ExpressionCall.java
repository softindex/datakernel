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
import java.util.Arrays;
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
	private final List<Expression> arguments = new ArrayList<>();

	ExpressionCall(Expression owner, String methodName, Expression... arguments) {
		this.owner = owner;
		this.methodName = methodName;
		this.arguments.addAll(Arrays.asList(arguments));
	}

	@Override
	public Type type(Context ctx) {
		List<Class<?>> argumentClasses = new ArrayList<>();
		List<Type> argumentTypes = new ArrayList<>();
		for (Expression argument : arguments) {
			argumentTypes.add(argument.type(ctx));
			if (argument.type(ctx).equals(getType(Object[].class))) {
				argumentClasses.add(Object[].class);
			} else {
				argumentClasses.add(getJavaType(ctx.getClassLoader(), argument.type(ctx)));
			}
		}
		Type returnType;
		try {
			if (ctx.getThisType().equals(owner.type(ctx))) {
				for (org.objectweb.asm.commons.Method method : ctx.getMethods().keySet()) {
					if (method.getName().equals(methodName)) {
						if (method.getArgumentTypes().length == arguments.size()) {
							Type[] methodTypes = method.getArgumentTypes();
							boolean isSame = true;
							for (int i = 0; i < arguments.size(); i++) {
								if (!methodTypes[i].equals(argumentTypes.get(i))) {
									isSame = false;
									break;
								}
							}
							if (isSame) return method.getReturnType();
						}
					}
				}
				throw new NoSuchMethodException();
			} else {
				Class<?> ownerJavaType = getJavaType(ctx.getClassLoader(), owner.type(ctx));
				Method method = ownerJavaType.getMethod(methodName, argumentClasses.toArray(new Class<?>[]{}));
				Class<?> returnClass = method.getReturnType();
				returnType = getType(returnClass);
			}

		} catch (NoSuchMethodException e) {
			throw new RuntimeException(format("No method %s.%s(%s). %s",
					owner.type(ctx).getClassName(),
					methodName,
					(!argumentClasses.isEmpty() ? argsToString(argumentClasses) : ""),
					exceptionInGeneratedClass(ctx)
			));
		}

		return returnType;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		owner.load(ctx);

		List<Class<?>> argumentClasses = new ArrayList<>();
		List<Type> argumentTypes = new ArrayList<>();
		for (Expression argument : arguments) {
			argument.load(ctx);
			argumentTypes.add(argument.type(ctx));
			argumentClasses.add(getJavaType(ctx.getClassLoader(), argument.type(ctx)));
		}

		Type returnType;
		try {
			if (ctx.getThisType().equals(owner.type(ctx))) {
				org.objectweb.asm.commons.Method method = null;
				for (org.objectweb.asm.commons.Method m : ctx.getMethods().keySet()) {
					if (m.getName().equals(methodName)) {
						if (m.getArgumentTypes().length == arguments.size()) {
							Type[] methodTypes = m.getArgumentTypes();
							boolean isSame = true;
							for (int i = 0; i < arguments.size(); i++) {
								if (!methodTypes[i].equals(argumentTypes.get(i))) {
									isSame = false;
									break;
								}
							}
							if (isSame) {
								method = m;
								break;
							}
						}
					}
				}
				if (method == null) throw new NoSuchMethodException();
				g.invokeVirtual(owner.type(ctx), method);
				return method.getReturnType();
			}
			Class<?> ownerJavaType = getJavaType(ctx.getClassLoader(), owner.type(ctx));
			Method method = ownerJavaType.getMethod(methodName, argumentClasses.toArray(new Class<?>[]{}));
			Class<?> returnClass = method.getReturnType();
			returnType = getType(returnClass);

			invokeVirtualOrInterface(g, ownerJavaType, new org.objectweb.asm.commons.Method(methodName,
					returnType, argumentTypes.toArray(new Type[]{})));

		} catch (NoSuchMethodException e) {
			throw new RuntimeException(format("No method %s.%s(%s). %s",
					owner.type(ctx).getClassName(),
					methodName,
					(!argumentClasses.isEmpty() ? argsToString(argumentClasses) : ""),
					exceptionInGeneratedClass(ctx)));
		}
		return returnType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionCall that = (ExpressionCall) o;

		if (arguments != null ? !arguments.equals(that.arguments) : that.arguments != null) return false;
		if (methodName != null ? !methodName.equals(that.methodName) : that.methodName != null) return false;
		if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = owner != null ? owner.hashCode() : 0;
		result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
		result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
		return result;
	}
}
