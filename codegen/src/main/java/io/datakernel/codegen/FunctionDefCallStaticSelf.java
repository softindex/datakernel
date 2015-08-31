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

import static io.datakernel.codegen.FunctionDefs.self;

public class FunctionDefCallStaticSelf implements FunctionDef {
	private final FunctionDef owner;
	private final String methodName;
	private final List<FunctionDef> arguments = new ArrayList<>();

	public FunctionDefCallStaticSelf(String methodName, FunctionDef... functionDefs) {
		this.owner = self();
		this.methodName = methodName;
		this.arguments.addAll(Arrays.asList(functionDefs));
	}

	@Override
	public Type type(Context ctx) {
		List<Type> argumentTypes = new ArrayList<>();
		for (FunctionDef argument : arguments) {
			argumentTypes.add(argument.type(ctx));
		}

		Set<Method> methods = ctx.getFunctionDefStaticMap().keySet();
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
		throw new IllegalArgumentException();
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type ownerType = owner.type(ctx);

		List<Type> argumentTypes = new ArrayList<>();
		for (FunctionDef argument : arguments) {
			argument.load(ctx);
			argumentTypes.add(argument.type(ctx));
		}

		Type returnType = type(ctx);
		g.invokeStatic(ownerType, new Method(methodName, returnType, argumentTypes.toArray(new Type[]{})));
		return returnType;
	}
}
