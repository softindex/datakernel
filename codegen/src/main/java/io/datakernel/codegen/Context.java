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

import java.util.List;
import java.util.Map;

/**
 * Contains information about a dynamic class
 */
public final class Context {
	private final DefiningClassLoader classLoader;
	private final GeneratorAdapter g;
	private final Type thisType;
	private final Method method;
	private final Class<?> mainClass;
	private final List<Class<?>> otherClasses;
	private final Map<String, Class<?>> fields;
	private final Map<String, Object> staticConstants;
	private final Type[] argumentTypes;
	private final Map<Method, Expression> methods;
	private final Map<Method, Expression> staticMethods;

	public Context(DefiningClassLoader classLoader, GeneratorAdapter g, Type thisType, Class<?> mainClass,
	               List<Class<?>> otherClasses, Map<String, Class<?>> fields, Map<String, Object> staticConstants,
	               Type[] argumentTypes, Method method, Map<Method, Expression> methods,
	               Map<Method, Expression> staticMethods) {
		this.classLoader = classLoader;
		this.g = g;
		this.method = method;
		this.mainClass = mainClass;
		this.otherClasses = otherClasses;
		this.argumentTypes = argumentTypes;
		this.thisType = thisType;
		this.fields = fields;
		this.staticConstants = staticConstants;
		this.methods = methods;
		this.staticMethods = staticMethods;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	public GeneratorAdapter getGeneratorAdapter() {
		return g;
	}

	public Class<?> getMainClass() {
		return mainClass;
	}

	public List<Class<?>> getOtherClasses() {
		return otherClasses;
	}

	public Type getThisType() {
		return thisType;
	}

	public Map<String, Class<?>> getFields() {
		return fields;
	}

	public Map<String, Object> getStaticConstants() {
		return staticConstants;
	}

	public void addStaticConstant(String field, Object value) {
		this.staticConstants.put(field, value);
	}

	public Type[] getArgumentTypes() {
		return argumentTypes;
	}

	public Type getArgumentType(int argument) {
		return argumentTypes[argument];
	}

	public Map<Method, Expression> getStaticMethods() {
		return staticMethods;
	}

	public Map<Method, Expression> getMethods() {
		return methods;
	}

	public Method getMethod() {
		return method;
	}
}
