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

import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.Utils.argsToString;
import static io.datakernel.codegen.Utils.exceptionInGeneratedClass;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

final class ExpressionCallStaticSelf implements Expression {
	private final String methodName;
	private final List<Expression> arguments;

	public ExpressionCallStaticSelf(String methodName, List<Expression> expressions) {
		this.methodName = checkNotNull(methodName);
		this.arguments = checkNotNull(expressions);
	}

	public Type type(Type[] argumentTypes, Context ctx) {
		for (Method m : ctx.getStaticMethods().keySet()) {
			if (m.getName().equals(methodName) && m.getArgumentTypes().length == argumentTypes.length) {
				Type[] methodTypes = m.getArgumentTypes();
				boolean found = true;
				for (int i = 0; i < argumentTypes.length; i++) {
					if (!methodTypes[i].equals(argumentTypes[i])) {
						found = false;
						break;
					}
				}
				if (found) {
					return m.getReturnType();
				}
			}
		}
		throw new RuntimeException(format("No method %s.%s(%s). %s",
				ctx.getThisType().getClassName(),
				methodName,
				argumentTypes.length != 0 ?
						argsToString(Arrays.stream(argumentTypes).collect(toList())) :
						"",
				exceptionInGeneratedClass(ctx)));
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type[] argumentTypes = new Type[arguments.size()];
		for (int i = 0; i < arguments.size(); i++) {
			Type argumentType = arguments.get(i).load(ctx);
			argumentTypes[i] = argumentType;
		}

		Type returnType = type(argumentTypes, ctx);
		g.invokeStatic(ctx.getThisType(), new Method(methodName, returnType, argumentTypes));
		return returnType;
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionCallStaticSelf that = (ExpressionCallStaticSelf) o;

		if (!methodName.equals(that.methodName)) return false;
		if (!arguments.equals(that.arguments)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = methodName.hashCode();
		result = 31 * result + arguments.hashCode();
		return result;
	}
}
