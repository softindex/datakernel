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

import com.google.common.reflect.TypeToken;
import io.datakernel.asm.DefiningClassLoader;
import io.datakernel.asm.DefiningClassWriter;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.concat;
import static io.datakernel.codegen.Utils.loadAndCast;
import static java.util.Arrays.asList;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getInternalName;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Intends for dynamic description of the behaviour of the object in runtime
 *
 * @param <T> type of item
 */
@SuppressWarnings("unchecked")
public class AsmFunctionFactory<T> {
	private static final Logger logger = getLogger(AsmFunctionFactory.class);

	public static final String ASM_FUNCTION = "asm.Function";
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private final DefiningClassLoader classLoader;

	private final Class<T> type;
	private final Map<String, Class<?>> fields = new LinkedHashMap<>();
	private final Map<Method, FunctionDef> functionDefMap = new LinkedHashMap<>();

	private class AsmFunctionKey {
		private final Map<String, Class<?>> fields;
		private final Map<Method, FunctionDef> functionDefMap;

		public AsmFunctionKey(Map<String, Class<?>> fields, Map<Method, FunctionDef> functionDefMap) {
			this.fields = fields;
			this.functionDefMap = functionDefMap;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			AsmFunctionKey that = (AsmFunctionKey) o;

			if (!fields.equals(that.fields)) return false;
			return functionDefMap.equals(that.functionDefMap);
		}

		@Override
		public int hashCode() {
			int result = fields.hashCode();
			result = 31 * result + functionDefMap.hashCode();
			return result;
		}
	}

	/**
	 * Creates a new instance of AsmFunctionFactory
	 *
	 * @param classLoader class loader
	 * @param type        type of dynamic class
	 */
	public AsmFunctionFactory(DefiningClassLoader classLoader, Class<T> type) {
		this.classLoader = classLoader;
		this.type = type;
	}

	public AsmFunctionFactory(DefiningClassLoader classLoader, TypeToken<T> type) {
		this.classLoader = classLoader;
		this.type = (Class<T>) type.getRawType();
	}

	/**
	 * Creates a new field for a dynamic class
	 *
	 * @param field      name of field
	 * @param fieldClass type of field
	 * @return changed AsmFunctionFactory
	 */
	public AsmFunctionFactory<T> field(String field, Class<?> fieldClass) {
		fields.put(field, fieldClass);
		return this;
	}

/*
	public AsmFunctionFactory<T> method(String methodName, Class<?>[] methodParameters, FunctionDef functionDef) {
		try {
			java.lang.reflect.Method method = type.getMethod(methodName, methodParameters);
			functionDefMap.put(method, functionDef);
		} catch (NoSuchMethodException e) {
			throw propagate(e);
		}
		return this;
	}
*/

	/**
	 * Creates a new method for a dynamic class
	 *
	 * @param method      new method for class
	 * @param functionDef function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmFunctionFactory<T> method(Method method, FunctionDef functionDef) {
		functionDefMap.put(method, functionDef);
		return this;
	}

	/**
	 * Creates a new method for a dynamic class
	 *
	 * @param methodName    name of method
	 * @param returnClass   type which returns this method
	 * @param argumentTypes list of types of arguments
	 * @param functionDef   function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmFunctionFactory<T> method(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, FunctionDef functionDef) {
		Type[] types = new Type[argumentTypes.size()];
		for (int i = 0; i < argumentTypes.size(); i++) {
			types[i] = getType(argumentTypes.get(i));
		}
		return method(new Method(methodName, getType(returnClass), types), functionDef);
	}

	/**
	 * CCreates a new method for a dynamic class
	 *
	 * @param methodName  name of method
	 * @param functionDef function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmFunctionFactory<T> method(String methodName, FunctionDef functionDef) {
		if (methodName.contains("(")) {
			Method method = Method.getMethod(methodName);
			return method(method, functionDef);
		}

		Method foundMethod = null;
		LinkedHashSet<java.lang.reflect.Method> methods = new LinkedHashSet<>();
		for (java.lang.reflect.Method m : concat(asList(type.getMethods()), asList(type.getDeclaredMethods()), asList(Object.class.getMethods()))) {
			if (m.getName().equals(methodName)) {
				Method method = getMethod(m);
				if (foundMethod != null && !method.equals(foundMethod))
					throw new IllegalArgumentException("Method " + method + " collides with " + foundMethod);
				foundMethod = method;
			}
		}
		checkArgument(foundMethod != null, "Could not find method '" + methodName + "'");
		return method(foundMethod, functionDef);
	}

	/**
	 * Returns a new class which is created in a dynamic way
	 *
	 * @return completed class
	 */
	public Class<T> defineClass() {
		synchronized (classLoader) {
			AsmFunctionKey key = new AsmFunctionKey(fields, functionDefMap);
			Class<?> cachedClass = classLoader.getClassByKey(key);

			if (cachedClass != null) {
				logger.trace("Dynamic class cache hit.");
				return (Class<T>) cachedClass;
			}

			logger.trace("Dynamic class cache miss.");

			return defineNewClass(key);
		}
	}

	/**
	 * Returns a new class which is created in a dynamic way
	 *
	 * @param key key
	 * @return completed class
	 */
	private Class<T> defineNewClass(AsmFunctionKey key) {
		DefiningClassWriter cw = new DefiningClassWriter(classLoader);

		String className = ASM_FUNCTION + COUNTER.incrementAndGet();
		Type classType = getType('L' + className.replace('.', '/') + ';');

		if (type.isInterface()) {
			cw.visit(V1_1, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
					classType.getInternalName(),
					null,
					"java/lang/Object",
					new String[]{getInternalName(type)});
		} else {
			cw.visit(V1_1, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
					classType.getInternalName(),
					null,
					getInternalName(type),
					new String[]{});
		}

		{
			Method m = getMethod("void <init> ()");
			GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);
			g.loadThis();
			g.invokeConstructor(getType(Object.class), m);
			g.returnValue();
			g.endMethod();
		}

		for (String field : fields.keySet()) {
			Class<?> fieldClass = fields.get(field);
			cw.visitField(ACC_PUBLIC, field, getType(fieldClass).getDescriptor(), null, null);
		}

		for (Method m : functionDefMap.keySet()) {

			try {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);

				Context ctx = new Context(classLoader, g, classType, type, fields, functionDefMap.keySet(),
						m.getArgumentTypes());

				FunctionDef functionDef = functionDefMap.get(m);
				loadAndCast(ctx, functionDef, m.getReturnType());
				g.returnValue();

				g.endMethod();
			} catch (Exception e) {
				logger.error("Could not implement " + m, e);
			}
		}

		cw.visitEnd();

		logger.trace("Defining class " + className);

		Class<?> definedClass = classLoader.defineClass(className, cw.toByteArray());

		classLoader.addToCache(key, definedClass);

		return (Class<T>) definedClass;
	}

	/**
	 * Returns a new instance of a dynamic class
	 *
	 * @return new instance of the class which was created before in a dynamic way
	 */
	public T newInstance() {
		try {
			return defineClass().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw propagate(e);
		}
	}
}
