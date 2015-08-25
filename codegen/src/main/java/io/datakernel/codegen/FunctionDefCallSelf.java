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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.FunctionDefs.self;
import static io.datakernel.codegen.Utils.getJavaType;

public class FunctionDefCallSelf implements FunctionDef {
	private final FunctionDef owner;
	private final String methodName;
	private final Type returnType;
	private final List<FunctionDef> arguments = new ArrayList<>();

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefCallSelf that = (FunctionDefCallSelf) o;

		if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;
		if (methodName != null ? !methodName.equals(that.methodName) : that.methodName != null) return false;
		if (returnType != null ? !returnType.equals(that.returnType) : that.returnType != null) return false;
		return !(arguments != null ? !arguments.equals(that.arguments) : that.arguments != null);

	}

	@Override
	public int hashCode() {
		int result = owner != null ? owner.hashCode() : 0;
		result = 31 * result + (methodName != null ? methodName.hashCode() : 0);
		result = 31 * result + (returnType != null ? returnType.hashCode() : 0);
		result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
		return result;
	}

	FunctionDefCallSelf(String methodName, Type returnType, FunctionDef... arguments) {
		this.owner = self();
		this.methodName = methodName;
		this.returnType = returnType;
		this.arguments.addAll(Arrays.asList(arguments));
	}

	@Override
	public Type type(Context ctx) {
		return returnType;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		owner.load(ctx);
		List<Class<?>> argumentClasses = new ArrayList<>();
		List<Type> argumentTypes = new ArrayList<>();
		for (FunctionDef argument : arguments) {
			argument.load(ctx);
			argumentTypes.add(argument.type(ctx));
			argumentClasses.add(getJavaType(ctx.getClassLoader(), argument.type(ctx)));
		}

		g.invokeVirtual(owner.type(ctx),
				new org.objectweb.asm.commons.Method(methodName, returnType, argumentTypes.toArray(new Type[]{})));
		return returnType;
	}
}
