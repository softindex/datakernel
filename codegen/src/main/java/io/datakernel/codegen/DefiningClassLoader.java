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

import io.datakernel.codegen.ClassBuilder.AsmClassKey;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Represents a loader for defining dynamically generated classes.
 * Also contains cache, that speeds up loading of classes, which have the same structure as the ones already loaded.
 */
public final class DefiningClassLoader extends ClassLoader implements DefiningClassLoaderMBean {
	private final Map<AsmClassKey<?>, Class<?>> definedClasses = new HashMap<>();

	// region builders
	private DefiningClassLoader() {
	}

	private DefiningClassLoader(ClassLoader parent) {
		super(parent);
	}

	public static DefiningClassLoader create() {return new DefiningClassLoader();}

	public static DefiningClassLoader create(ClassLoader parent) {return new DefiningClassLoader(parent);}
	// endregion

	Class<?> defineClass(String name, AsmClassKey<?> key, byte[] b) {
		Class<?> definedClass = defineClass(name, b, 0, b.length);
		definedClasses.put(key, definedClass);
		return definedClass;
	}

	Class<?> getClassByKey(AsmClassKey<?> key) {
		return definedClasses.get(key);
	}

	// jmx
	@Override
	public int getDefinedClassesCount() {
		return definedClasses.size();
	}

	@Override
	synchronized public Map<String, Integer> getDefinedClassesByType() {
		Map<String, Integer> map = new HashMap<>();

		for (Map.Entry<AsmClassKey<?>, Class<?>> entry : definedClasses.entrySet()) {
			String type = asList(entry.getKey().getMainClass(), entry.getKey().getOtherClasses()).toString();
			Integer count = map.get(type);
			map.put(type, count == null ? 1 : count + 1);
		}

		return map;
	}

	@Override
	public String toString() {
		return "{classes=" + definedClasses.size() + ", byType=" + getDefinedClassesByType() + '}';
	}
}