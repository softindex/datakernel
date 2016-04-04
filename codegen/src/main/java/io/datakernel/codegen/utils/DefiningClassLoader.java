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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a loader for defining dynamically generated classes.
 * Also contains cache, that speeds up loading of classes, which have the same structure as the ones already loaded.
 */
public class DefiningClassLoader extends ClassLoader implements DefiningClassLoaderMBean {
	private final Map<Object, Class<?>> definedClasses = new HashMap<>();

	// jmx
	private String lastDefinedClass;

	public DefiningClassLoader() {
	}

	public DefiningClassLoader(ClassLoader parent) {
		super(parent);
	}

	public Class<?> defineClass(String name, Object key, byte[] b) {
		Class<?> definedClass = defineClass(name, b, 0, b.length);
		definedClasses.put(key, definedClass);
		lastDefinedClass = name;
		return definedClass;
	}

	public Class<?> getClassByKey(Object key) {
		return definedClasses.get(key);
	}

	// jmx
	@Override
	public int getDefinedClassesAmount() {
		return definedClasses.size();
	}

	@Override
	public String getLastDefinedClass() {
		return lastDefinedClass;
	}
}