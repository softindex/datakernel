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
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.datakernel.codegen.Expressions.self;
import static io.datakernel.codegen.Utils.*;
import static java.lang.String.format;

final class ExpressionCallStaticSelf implements Expression {
	private final Expression owner;
	private final String methodName;
	private final List<Expression> arguments = new ArrayList<>();

	public ExpressionCallStaticSelf(String methodName, Expression... expressions) {
		this.owner = self();
		this.methodName = methodName;
		this.arguments.addAll(Arrays.asList(expressions));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionCallStaticSelf that = (ExpressionCallStaticSelf) o;

		if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;
		if (methodName != null ? !methodName.equals(that.methodName) : that.methodName != null) return false;
		return !(arguments != null ? !arguments.equals(that.arguments) : that.arguments != null);

	}

	@Override
	public int hashCode() {
		int result = owner != null ? owner.hashCode() : 0;
		result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
		result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
		return result;
	}

	@Override
	public Type type(Context ctx) {
		List<Type> argumentTypes = new ArrayList<>();
		for (Expression argument : arguments) {
			argumentTypes.add(argument.type(ctx));
		}

		Set<Method> methods = ctx.getStaticMethods().keySet();
		for (Method m : methods) {
			if (m.getName().equals(methodName)) {
				if (m.getArgumentTypes().length == argumentTypes.size()) {
					Type[] methodTypes = m.getArgumentTypes();
					boolean isSame = true;
					for (int i = 0; i < argumentTypes.size(); i++) {
						if (!methodTypes[i].equals(argumentTypes.get(i))) {
							isSame = false;
							break;
						}
					}
					if (isSame) {
						return m.getReturnType();
					}
				}
			}
		}
		throw new RuntimeException(format("No method %s.%s(%s). %s",
				owner.type(ctx).getClassName(),
				methodName,
				(!argumentTypes.isEmpty() ? argsToString(argumentClasses(ctx, arguments)) : ""),
				exceptionInGeneratedClass(ctx)));
	}

	private static List<Class<?>> argumentClasses(Context ctx, List<Expression> expressions) {
		List<Class<?>> classList = new ArrayList<>();
		for (Expression expression : expressions) {
			classList.add(getJavaType(ctx.getClassLoader(), expression.type(ctx)));
		}
		return classList;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type ownerType = owner.type(ctx);

		List<Type> argumentTypes = new ArrayList<>();
		for (Expression argument : arguments) {
			argument.load(ctx);
			argumentTypes.add(argument.type(ctx));
		}

		Type returnType = type(ctx);
		g.invokeStatic(ownerType, new Method(methodName, returnType, argumentTypes.toArray(new Type[]{})));
		return returnType;
	}
}
