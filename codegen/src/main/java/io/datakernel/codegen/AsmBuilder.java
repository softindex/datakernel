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
import io.datakernel.codegen.utils.DefiningClassWriter;
import io.datakernel.codegen.utils.Preconditions;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.codegen.Utils.loadAndCast;
import static java.util.Arrays.asList;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getInternalName;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Intends for dynamic description of the behaviour of the object in runtime
 *
 * @param <T> type of item
 */
@SuppressWarnings("unchecked")
public class AsmBuilder<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final String DEFAULT_CLASS_NAME = AsmBuilder.class.getPackage().getName() + ".Class";
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private final DefiningClassLoader classLoader;
	private Path bytecodeSaveDir;

	private final ClassScope<T> scope;
	
	private final Map<String, Class<?>> fields = new LinkedHashMap<>();
	private final Map<String, Class<?>> staticFields = new LinkedHashMap<>();
	private final Map<Method, Expression> expressionMap = new LinkedHashMap<>();
	private final Map<Method, Expression> expressionStaticMap = new LinkedHashMap<>();

	public AsmBuilder<T> setBytecodeSaveDir(Path bytecodeSaveDir) {
		this.bytecodeSaveDir = bytecodeSaveDir;
		return this;
	}

	public static class AsmClassKey<T> {
		private final Set<Class<?>> parentClasses;
		private final Map<String, Class<?>> fields;
		private final Map<String, Class<?>> staticFields;
		private final Map<Method, Expression> expressionMap;
		private final Map<Method, Expression> expressionStaticMap;

		public AsmClassKey(Set<Class<?>> parentClasses, Map<String, Class<?>> fields, Map<String, Class<?>> staticFields,
		                   Map<Method, Expression> expressionMap, Map<Method, Expression> expressionStaticMap) {
			this.parentClasses = parentClasses;
			this.fields = fields;
			this.staticFields = staticFields;
			this.expressionMap = expressionMap;
			this.expressionStaticMap = expressionStaticMap;
		}

		public Set<Class<?>> getParentClasses() {
			return parentClasses;
		}

		@Override
		public String toString() {
			return "AsmClassKey{" +
					"parentClasses=" + parentClasses +
					", fields=" + fields +
					", staticFields=" + staticFields +					
					", expressionMap=" + expressionMap +
					", expressionStaticMap=" + expressionStaticMap +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AsmClassKey<?> that = (AsmClassKey<?>) o;
			return  Objects.equals(parentClasses, that.parentClasses) &&
					Objects.equals(fields, that.fields) &&
					Objects.equals(staticFields, that.staticFields) &&					
					Objects.equals(expressionMap, that.expressionMap) &&
					Objects.equals(expressionStaticMap, that.expressionStaticMap);
		}

		@Override
		public int hashCode() {
			return Objects.hash(parentClasses, fields, expressionMap, expressionStaticMap);
		}
	}

	/**
	 * Creates a new instance of AsmFunctionFactory
	 *
	 * @param classLoader class loader
	 * @param type        type of dynamic class
	 */
	public AsmBuilder(DefiningClassLoader classLoader, Class<T> type) {
		this(classLoader, type, Collections.EMPTY_LIST);
	}

	public AsmBuilder(DefiningClassLoader classLoader, Class<T> mainType, List<Class<?>> types) {
		this.classLoader = classLoader;
		this.scope = new ClassScope<>(mainType, types);
	}

	/**
	 * Creates a new field for a dynamic class
	 *
	 * @param field      name of field
	 * @param fieldClass type of field
	 * @return changed AsmFunctionFactory
	 */
	public AsmBuilder<T> field(String field, Class<?> fieldClass) {
		fields.put(field, fieldClass);
		scope.addField(field, fieldClass);
		return this;
	}
	
	/**
	 * Creates a new static field for a dynamic class
	 *  
	 * @param field
	 * @param fieldClass
	 * @return
	 */
	public AsmBuilder<T> staticField(String field, Class<?> fieldClass) {
		staticFields.put(field, fieldClass);
		scope.addStaticField(field, fieldClass);
		return this;
	}

	/**
	 * Creates a new method for a dynamic class
	 *
	 * @param method     new method for class
	 * @param expression function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmBuilder<T> method(Method method, Expression expression) {
		expressionMap.put(method, expression);
		scope.addMethod(method);
		return this;
	}

	public AsmBuilder<T> staticMethod(Method method, Expression expression) {
		expressionStaticMap.put(method, expression);
		scope.addStaticMethod(method);
		return this;
	}

	/**
	 * Creates a new method for a dynamic class
	 *
	 * @param methodName    name of method
	 * @param returnClass   type which returns this method
	 * @param argumentTypes list of types of arguments
	 * @param expression    function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmBuilder<T> method(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		Type[] types = new Type[argumentTypes.size()];
		for (int i = 0; i < argumentTypes.size(); i++) {
			types[i] = getType(argumentTypes.get(i));
		}
		return method(new Method(methodName, getType(returnClass), types), expression);
	}

	/**
	 * Create a new static method for a dynamic class
	 * 
	 * @param methodName    name of method
	 * @param returnClass   type which returns this method
	 * @param argumentTypes list of types of arguments
	 * @param expression    function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmBuilder<T> staticMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		Type[] types = new Type[argumentTypes.size()];
		for (int i = 0; i < argumentTypes.size(); i++) {
			types[i] = getType(argumentTypes.get(i));
		}
		return staticMethod(new Method(methodName, getType(returnClass), types), expression);
	}
	
	/**
	 * Create a new static initialization block for a dynamic class. Overwrites the existing one.
	 * 
	 * @param expression function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmBuilder<T> staticInitializationBlock(Expression expression) {
		return staticMethod("<clinit>", void.class, Collections.EMPTY_LIST, expression);
	}
		
		

	/**
	 * Creates a new method for a dynamic class. The method must be part of the provided interfaces or abstract class.
	 *
	 * @param methodName name of method
	 * @param expression function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public AsmBuilder<T> method(String methodName, Expression expression) {
		if (methodName.contains("(")) {
			Method method = Method.getMethod(methodName);
			return method(method, expression);
		}

		Method foundMethod = null;
		List<List<java.lang.reflect.Method>> listOfMethods = new ArrayList<>();
		listOfMethods.add(asList(Object.class.getMethods()));
		for (Class<?> type : scope.getParentClasses()) {
			listOfMethods.add(asList(type.getMethods()));
			listOfMethods.add(asList(type.getDeclaredMethods()));
		}
		for (List<java.lang.reflect.Method> list : listOfMethods) {
			for (java.lang.reflect.Method m : list) {
				if (m.getName().equals(methodName)) {
					Method method = getMethod(m);
					if (foundMethod != null && !method.equals(foundMethod))
						throw new IllegalArgumentException("Method " + method + " collides with " + foundMethod);
					foundMethod = method;
				}
			}
		}
		Preconditions.check(foundMethod != null, "Could not find method '" + methodName + "'");
		return method(foundMethod, expression);
	}

	/**
	 * Returns a new class which is created in a dynamic way
	 *
	 * @return completed class
	 */
	public Class<T> defineClass() {
		return defineClass(null);
	}

	public Class<T> defineClass(String className) {
		synchronized (classLoader) {
			AsmClassKey<T> key = new AsmClassKey<>(scope.getParentClasses(), fields, staticFields, expressionMap, expressionStaticMap);
			Class<?> cachedClass = classLoader.getClassByKey(key);

			if (cachedClass != null) {
				logger.trace("Fetching {} for key {} from cache", cachedClass, key);
				return (Class<T>) cachedClass;
			}

			return defineNewClass(key, className);
		}
	}

	/**
	 * Returns a new class which is created in a dynamic way
	 *
	 * @param key key
	 * @return completed class
	 */
	private Class<T> defineNewClass(AsmClassKey<T> key, String newClassName) {
		DefiningClassWriter cw = new DefiningClassWriter(classLoader);

		String className;
		if (newClassName == null) {
			className = DEFAULT_CLASS_NAME + COUNTER.incrementAndGet();
		} else {
			className = newClassName;
		}

		Type classType = getType('L' + className.replace('.', '/') + ';');

		// contains all classes (abstract and interfaces)
		final Set<Class<?>> parentClasses = scope.getParentClasses();
		final String[] internalNames = new String[parentClasses.size()];
		int pos = 0;
		for (Class<?> clazz : parentClasses)
			internalNames[pos++] = getInternalName(clazz);		
	
		if (scope.getMainType().isInterface()) {
			cw.visit(V1_6, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
					classType.getInternalName(),
					null,
					"java/lang/Object",
					internalNames);
		} else {
			cw.visit(V1_6, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
					classType.getInternalName(),
					null,
					internalNames[0],
					Arrays.copyOfRange(internalNames, 1, internalNames.length));
		}

		{
			Method m = getMethod("void <init> ()");
			GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);
			g.loadThis();

			if (scope.getMainType().isInterface()) {
				g.invokeConstructor(getType(Object.class), m);
			} else {
				g.invokeConstructor(getType(scope.getMainType()), m);
			}

			g.returnValue();
			g.endMethod();
		}

		for (String field : fields.keySet()) {
			Class<?> fieldClass = fields.get(field);
			cw.visitField(ACC_PUBLIC, field, getType(fieldClass).getDescriptor(), null, null);
		}
		
		for (String field : staticFields.keySet()) {
			Class<?> fieldClass = staticFields.get(field);
			cw.visitField(ACC_PUBLIC + ACC_STATIC, field, getType(fieldClass).getDescriptor(), null, null);
		}

		for (Method m : expressionStaticMap.keySet()) {
			try {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC + ACC_STATIC + ACC_FINAL, m, null, null, cw);

				Context ctx = new Context(classLoader, g, classType, scope.getParentClasses(), Collections.EMPTY_MAP, scope.getStaticFields(), m.getArgumentTypes(), m, scope.getMethods(), scope.getStaticMethods());

				Expression expression = expressionStaticMap.get(m);
				loadAndCast(ctx, expression, m.getReturnType());
				g.returnValue();

				g.endMethod();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		for (Method m : expressionMap.keySet()) {
			try {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);

				Context ctx = new Context(classLoader, g, classType, scope.getParentClasses(), scope.getFields(), scope.getStaticFields(), m.getArgumentTypes(), m, scope.getMethods(), scope.getStaticMethods());

				Expression expression = expressionMap.get(m);
				loadAndCast(ctx, expression, m.getReturnType());
				g.returnValue();

				g.endMethod();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (bytecodeSaveDir != null) {
			try (FileOutputStream fos = new FileOutputStream(bytecodeSaveDir.resolve(className + ".class").toFile())) {
				fos.write(cw.toByteArray());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		cw.visitEnd();

		Class<?> definedClass = classLoader.defineClass(className, key, cw.toByteArray());
		logger.trace("Defined new {} for key {}", definedClass, key);
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
			throw new RuntimeException(e);
		}
	}
}
