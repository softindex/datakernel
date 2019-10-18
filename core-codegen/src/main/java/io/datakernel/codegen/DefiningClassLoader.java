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

import java.util.*;

import static io.datakernel.common.collection.CollectionUtils.concat;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

/**
 * Represents a loader for defining dynamically generated classes.
 * Also contains cache, that speeds up loading of classes, which have the same structure as the ones already loaded.
 */
public final class DefiningClassLoader extends ClassLoader implements DefiningClassLoaderMBean {

	public static final class ClassKey {
		private final Class<?> superclass;
		private final Set<Class<?>> interfaces;
		private final List<Object> parameters;

		ClassKey(Class<?> superclass, Set<Class<?>> interfaces, List<Object> parameters) {
			this.superclass = superclass;
			this.interfaces = interfaces;
			this.parameters = parameters;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ClassKey key = (ClassKey) o;
			return superclass.equals(key.superclass) &&
					interfaces.equals(key.interfaces) &&
					parameters.equals(key.parameters);
		}

		@Override
		public int hashCode() {
			return Objects.hash(superclass, interfaces, parameters);
		}
	}

	private final Map<ClassKey, Class<?>> definedClasses = new HashMap<>();

	// region builders
	private DefiningClassLoader() {
	}

	private DefiningClassLoader(ClassLoader parent) {
		super(parent);
	}

	public static DefiningClassLoader create() {return new DefiningClassLoader();}

	public static DefiningClassLoader create(ClassLoader parent) {return new DefiningClassLoader(parent);}
	// endregion

	public Class<?> defineClass(ClassKey key, String className, byte[] bytecode) {
		Class<?> definedClass = defineClass(className, bytecode, 0, bytecode.length);
		definedClasses.put(key, definedClass);
		return definedClass;
	}

	Class<?> getClassByKey(ClassKey key) {
		return definedClasses.get(key);
	}

	// jmx
	@Override
	public int getDefinedClassesCount() {
		return definedClasses.size();
	}

	@Override
	synchronized public Map<String, Long> getDefinedClassesCountByType() {
		return definedClasses.keySet().stream()
				.map(key -> concat(singletonList(key.superclass), key.interfaces).toString())
				.collect(groupingBy(identity(), counting()));
	}

	@Override
	public String toString() {
		return "{classes=" + definedClasses.size() + ", byType=" + getDefinedClassesCountByType() + '}';
	}
}
