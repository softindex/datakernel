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

import java.util.Map;
import java.util.Set;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import io.datakernel.codegen.utils.DefiningClassLoader;

/**
 * Contains information about a dynamic class
 */
public final class Context {
	private final DefiningClassLoader classLoader;
	private final GeneratorAdapter g;
	private final Type thisType;
	private final Method newMethod;
	private final Set<Class<?>> parentClasses;
	private final Map<String, Class<?>> staticFields;
	private final Map<String, Class<?>> thisFields;
	private final Type[] argumentTypes;
	private final Set<Method> methods;
	private final Set<Method> staticMethods;

	public Context(DefiningClassLoader classLoader, GeneratorAdapter g, Type thisType, Set<Class<?>> parantClasses, Map<String, Class<?>> thisFields, 
					Map<String, Class<?>> staticFields, Type[] argumentTypes, Method method, Set<Method> methods, Set<Method> staticMethods) {
		this.classLoader = classLoader;
		this.g = g;
		this.newMethod = method;
		this.parentClasses = parantClasses;
		this.argumentTypes = argumentTypes;
		this.thisType = thisType;
		this.staticFields = staticFields;
		this.thisFields = thisFields;
		this.methods = methods;
		this.staticMethods = staticMethods;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	public GeneratorAdapter getGeneratorAdapter() {
		return g;
	}

	public Set<Class<?>> getOtherClasses() {
		return parentClasses;
	}

	public Type getThisType() {
		return thisType;
	}
	
	public Map<String, Class<?>> getStaticFields() {
		return staticFields;
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

	public Set<Method> getStaticMethods() {
		return staticMethods;
	}

	public Set<Method> getMethods() {
		return methods;
	}

	public Method getNewMethod() {
		return newMethod;
	}
}
