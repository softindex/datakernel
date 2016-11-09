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

import io.datakernel.codegen.utils.DefiningClassWriter;
import io.datakernel.codegen.utils.Preconditions;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
public final class ClassBuilder<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final String DEFAULT_CLASS_NAME = ClassBuilder.class.getPackage().getName() + ".Class";
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private final DefiningClassLoader classLoader;

	private final Class<T> mainClass;
	private final List<Class<?>> otherClasses;
	private Path bytecodeSaveDir;

	private String className;

	private final Map<String, Class<?>> fields = new LinkedHashMap<>();
	private final Map<String, Class<?>> staticFields = new LinkedHashMap<>();
	private final Map<String, Object> staticConstants = new LinkedHashMap<>();
	private final Map<Method, Expression> methods = new LinkedHashMap<>();
	private final Map<Method, Expression> staticMethods = new LinkedHashMap<>();

	public static class AsmClassKey<T> {
		private final Class<T> mainClass;
		private final List<Class<?>> otherClasses;
		private final Map<String, Class<?>> fields;
		private final Map<Method, Expression> expressionMap;
		private final Map<Method, Expression> expressionStaticMap;

		public AsmClassKey(Class<T> mainClass, List<Class<?>> otherClasses, Map<String, Class<?>> fields,
		                   Map<Method, Expression> expressionMap, Map<Method, Expression> expressionStaticMap) {
			this.mainClass = mainClass;
			this.otherClasses = otherClasses;
			this.fields = fields;
			this.expressionMap = expressionMap;
			this.expressionStaticMap = expressionStaticMap;
		}

		public Class<T> getMainClass() {
			return mainClass;
		}

		public List<Class<?>> getOtherClasses() {
			return otherClasses;
		}

		@Override
		public String toString() {
			return "AsmClassKey{" +
					"mainType=" + mainClass +
					", otherTypes" + otherClasses +
					", fields=" + fields +
					", expressionMap=" + expressionMap +
					", expressionStaticMap=" + expressionStaticMap +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AsmClassKey that = (AsmClassKey) o;
			return Objects.equals(mainClass, that.mainClass) &&
					Objects.equals(otherClasses, that.otherClasses) &&
					Objects.equals(fields, that.fields) &&
					Objects.equals(expressionMap, that.expressionMap) &&
					Objects.equals(expressionStaticMap, that.expressionStaticMap);
		}

		@Override
		public int hashCode() {
			return Objects.hash(mainClass, otherClasses, fields, expressionMap, expressionStaticMap);
		}
	}

	// region builders

	/**
	 * Creates a new instance of AsmFunctionFactory
	 *
	 * @param classLoader class loader
	 * @param type        type of dynamic class
	 */
	private ClassBuilder(DefiningClassLoader classLoader, Class<T> type) {
		this(classLoader, type, Collections.EMPTY_LIST);
	}

	private ClassBuilder(DefiningClassLoader classLoader, Class<T> mainType, List<Class<?>> types) {
		this.classLoader = classLoader;
		this.mainClass = mainType;
		this.otherClasses = types;
	}

	public static <T> ClassBuilder<T> create(DefiningClassLoader classLoader, Class<T> type) {
		return new ClassBuilder<>(classLoader, type);
	}

	public static <T> ClassBuilder<T> create(DefiningClassLoader classLoader, Class<T> mainType, List<Class<?>> types) {
		return new ClassBuilder<T>(classLoader, mainType, types);
	}

	public ClassBuilder<T> withBytecodeSaveDir(Path bytecodeSaveDir) {
		this.bytecodeSaveDir = bytecodeSaveDir;
		return this;
	}

	/**
	 * Creates a new field for a dynamic class
	 *
	 * @param field      name of field
	 * @param fieldClass type of field
	 * @return changed AsmFunctionFactory
	 */
	public ClassBuilder<T> withField(String field, Class<?> fieldClass) {
		fields.put(field, fieldClass);
		return this;
	}

	/**
	 * Creates a new method for a dynamic class
	 *
	 * @param method     new method for class
	 * @param expression function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public ClassBuilder<T> withMethod(Method method, Expression expression) {
		methods.put(method, expression);
		return this;
	}

	public ClassBuilder<T> withStaticMethod(Method method, Expression expression) {
		staticMethods.put(method, expression);
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
	public ClassBuilder<T> withMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		Type[] types = new Type[argumentTypes.size()];
		for (int i = 0; i < argumentTypes.size(); i++) {
			types[i] = getType(argumentTypes.get(i));
		}
		return withMethod(new Method(methodName, getType(returnClass), types), expression);
	}

	public ClassBuilder<T> withStaticMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		Type[] types = new Type[argumentTypes.size()];
		for (int i = 0; i < argumentTypes.size(); i++) {
			types[i] = getType(argumentTypes.get(i));
		}
		return withStaticMethod(new Method(methodName, getType(returnClass), types), expression);
	}

	public ClassBuilder<T> withStaticField(String fieldName, Class<?> type, Object value) {
		this.staticFields.put(fieldName, type);
		this.staticConstants.put(fieldName, value);
		return this;
	}

	/**
	 * CCreates a new method for a dynamic class
	 *
	 * @param methodName name of method
	 * @param expression function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public ClassBuilder<T> withMethod(String methodName, Expression expression) {
		if (methodName.contains("(")) {
			Method method = Method.getMethod(methodName);
			return withMethod(method, expression);
		}

		Method foundMethod = null;
		LinkedHashSet<java.lang.reflect.Method> methods = new LinkedHashSet<>();
		List<List<java.lang.reflect.Method>> listOfMethods = new ArrayList<>();
		listOfMethods.add(asList(Object.class.getMethods()));
		listOfMethods.add(asList(mainClass.getMethods()));
		listOfMethods.add(asList(mainClass.getDeclaredMethods()));
		for (Class<?> type : otherClasses) {
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
		return withMethod(foundMethod, expression);
	}

	public ClassBuilder<T> withClassName(String name) {
		this.className = name;
		return this;
	}

	// endregion
	public Class<T> build() {
		synchronized (classLoader) {
			AsmClassKey key = new AsmClassKey(mainClass, otherClasses, fields, methods, staticMethods);
			Class<?> cachedClass = classLoader.getClassByKey(key);

			if (cachedClass != null) {
				logger.trace("Fetching {} for key {} from cache", cachedClass, key);
				return (Class<T>) cachedClass;
			}

			Class<T> newClass = defineNewClass(key);
			for (String staticField : staticConstants.keySet()) {
				Object staticValue = staticConstants.get(staticField);
				try {
					Field field = newClass.getField(staticField);
					field.set(null, staticValue);
				} catch (NoSuchFieldException | IllegalAccessException e) {
					throw new AssertionError(e);
				}
			}
			return newClass;
		}
	}

	private Class<T> defineNewClass(AsmClassKey key) {
		DefiningClassWriter cw = DefiningClassWriter.create(classLoader);

		String actualClassName;
		if (className == null) {
			actualClassName = DEFAULT_CLASS_NAME + COUNTER.incrementAndGet();
		} else {
			actualClassName = className;
		}

		Type classType = getType('L' + actualClassName.replace('.', '/') + ';');

		final String[] internalNames = new String[1 + otherClasses.size()];
		internalNames[0] = getInternalName(mainClass);
		for (int i = 0; i < otherClasses.size(); i++) {
			internalNames[1 + i] = getInternalName(otherClasses.get(i));
		}
		if (mainClass.isInterface()) {
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

			if (mainClass.isInterface()) {
				g.invokeConstructor(getType(Object.class), m);
			} else {
				g.invokeConstructor(getType(mainClass), m);
			}

			g.returnValue();
			g.endMethod();
		}

		for (String field : fields.keySet()) {
			Class<?> fieldClass = fields.get(field);
			cw.visitField(ACC_PUBLIC, field, getType(fieldClass).getDescriptor(), null, null);
		}

		for (Method m : staticMethods.keySet()) {
			try {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC + ACC_STATIC + ACC_FINAL, m, null, null, cw);

				Context ctx = new Context(classLoader, g, classType, mainClass, otherClasses, fields, staticConstants, m.getArgumentTypes(), m, methods, staticMethods);

				Expression expression = staticMethods.get(m);
				loadAndCast(ctx, expression, m.getReturnType());
				g.returnValue();

				g.endMethod();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		for (Method m : methods.keySet()) {
			try {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);

				Context ctx = new Context(classLoader, g, classType, mainClass, otherClasses, fields, staticConstants, m.getArgumentTypes(), m, methods, staticMethods);

				Expression expression = methods.get(m);
				loadAndCast(ctx, expression, m.getReturnType());
				g.returnValue();

				g.endMethod();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		for (String staticField : staticFields.keySet()) {
			cw.visitField(ACC_PUBLIC + ACC_STATIC, staticField, getType(staticFields.get(staticField)).getDescriptor(), null, null);
		}

		for (String staticField : staticConstants.keySet()) {
			cw.visitField(ACC_PUBLIC + ACC_STATIC, staticField, getType(staticConstants.get(staticField).getClass()).getDescriptor(), null, null);
		}

		if (bytecodeSaveDir != null) {
			try (FileOutputStream fos = new FileOutputStream(bytecodeSaveDir.resolve(actualClassName + ".class").toFile())) {
				fos.write(cw.toByteArray());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		cw.visitEnd();

		Class<?> definedClass = classLoader.defineClass(actualClassName, key, cw.toByteArray());
		logger.trace("Defined new {} for key {}", definedClass, key);
		return (Class<T>) definedClass;
	}

	public T buildClassAndCreateNewInstance() {
		try {
			return build().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public T buildClassAndCreateNewInstance(Object... constructorParameters) {
		Class[] constructorParameterTypes = new Class[constructorParameters.length];
		for (int i = 0; i < constructorParameters.length; i++) {
			constructorParameterTypes[i] = constructorParameters[i].getClass();
		}
		return buildClassAndCreateNewInstance(constructorParameterTypes, constructorParameters);
	}

	public T buildClassAndCreateNewInstance(Class[] constructorParameterTypes, Object[] constructorParameters) {
		try {
			return build().getConstructor(constructorParameterTypes).newInstance(constructorParameters);
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}
}
