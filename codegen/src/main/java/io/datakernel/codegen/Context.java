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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
	private final Map<FunctionDef, Integer> cache = new HashMap<>();
	private final Map<String, Integer> namedLocals = new HashMap<>();

	public Context(DefiningClassLoader classLoader, GeneratorAdapter g, Type thisType, Class<?> thisSuperclass, Map<String, Class<?>> thisFields,
	               Set<Method> methods, Type[] argumentTypes) {
		this.classLoader = classLoader;
		this.g = g;
		this.thisSuperclass = thisSuperclass;
		this.argumentTypes = argumentTypes;
		this.thisType = thisType;
		this.thisFields = thisFields;
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

	public void putCache(FunctionDef functionDef, Integer localVar) {
		this.cache.put(functionDef, localVar);
	}

	public Integer getCache(FunctionDef functionDef) {
		return cache.get(functionDef);
	}

	public void putLocal(String name, VarLocal varLocal) {
		this.namedLocals.put(name, varLocal.getLocal());
	}

	public VarLocal newLocal(String name, Type type) {
		int local = g.newLocal(type);
		VarLocal var = new VarLocal(local);
		putLocal(name, var);
		return var;
	}

	public VarLocal getLocal(String name) {
		int local = this.namedLocals.get(name);
		return new VarLocal(local);
	}

	public boolean hasLocal(String name) {
		return this.namedLocals.containsKey(name);
	}
}
