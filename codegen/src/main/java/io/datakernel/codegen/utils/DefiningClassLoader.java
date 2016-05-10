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

package io.datakernel.codegen.utils;

import io.datakernel.codegen.AsmBuilder.AsmClassKey;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a loader for defining dynamically generated classes.
 * Also contains cache, that speeds up loading of classes, which have the same structure as the ones already loaded.
 */
public class DefiningClassLoader extends ClassLoader implements DefiningClassLoaderMBean {
	private final Map<AsmClassKey<?>, Class<?>> definedClasses = new HashMap<>();

	public DefiningClassLoader() {
	}

	public DefiningClassLoader(ClassLoader parent) {
		super(parent);
	}

	public Class<?> defineClass(String name, AsmClassKey<?> key, byte[] b) {
		Class<?> definedClass = defineClass(name, b, 0, b.length);
		definedClasses.put(key, definedClass);
		return definedClass;
	}

	public Class<?> getClassByKey(AsmClassKey<?> key) {
		return definedClasses.get(key);
	}

	// jmx
	@Override
	public int getDefinedClassesCount() {
		return definedClasses.size();
	}

	@Override
	public Map<String, String> getDefinedClasses() {
		Map<String, String> map = new HashMap<>(definedClasses.size());

		for (Map.Entry<AsmClassKey<?>, Class<?>> entry : definedClasses.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().toString());
		}

		return map;
	}

	@Override
	public Map<String, Integer> getDefinedClassesByType() {
		Map<String, Integer> map = new HashMap<>();

		for (Map.Entry<AsmClassKey<?>, Class<?>> entry : definedClasses.entrySet()) {
			String type = entry.getKey().getType().toString();
			Integer count = map.get(type);
			map.put(type, count == null ? 1 : count + 1);
		}

		return map;
	}

	@Override
	public String toString() {
		return "DefiningClassLoader{" +
				"classes=" + definedClasses.size() +
				", definedClassesByType=" + getDefinedClassesByType() +
				'}';
	}
}