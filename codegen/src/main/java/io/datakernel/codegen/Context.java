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

import io.datakernel.codegen.utils.DefiningClassLoader;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.Map;

/**
 * Contains information about a dynamic class
 */
public final class Context {
	private final DefiningClassLoader classLoader;
	private final GeneratorAdapter g;
	private final Type thisType;
	private final Class<?> thisSuperclass;
	private final Map<String, Class<?>> thisFields;
	private final Type[] argumentTypes;
	private final Map<Method, Expression> methodToExpression;
	private final Map<Method, Expression> staticMethodToExpression;

	public Context(DefiningClassLoader classLoader, GeneratorAdapter g, Type thisType, Class<?> thisSuperclass, Map<String, Class<?>> thisFields,
	               Type[] argumentTypes, Map<Method, Expression> methodToExpression, Map<Method, Expression> staticMethodToExpression) {
		this.classLoader = classLoader;
		this.g = g;
		this.thisSuperclass = thisSuperclass;
		this.argumentTypes = argumentTypes;
		this.thisType = thisType;
		this.thisFields = thisFields;
		this.methodToExpression = methodToExpression;
		this.staticMethodToExpression = staticMethodToExpression;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	public GeneratorAdapter getGeneratorAdapter() {
		return g;
	}

	public Type getThisType() {
		return thisType;
	}

	public Class<?> getThisSuperclass() {
		return thisSuperclass;
	}

	public Map<String, Class<?>> getThisFields() {
		return thisFields;
	}

	public Type[] getArgumentTypes() {
		return argumentTypes;
	}

	public Type getArgumentType(int argument) {
		return argumentTypes[argument];
	}

	public Map<Method, Expression> getStaticMethodToExpression() {
		return staticMethodToExpression;
	}

	public Map<Method, Expression> getMethodToExpression() {
		return methodToExpression;
	}
}
