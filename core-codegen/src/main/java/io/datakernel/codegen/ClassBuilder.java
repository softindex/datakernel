/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.codegen.DefiningClassLoader.ClassKey;
import io.datakernel.codegen.utils.DefiningClassWriter;
import io.datakernel.common.Initializable;
import io.datakernel.common.collection.CollectionUtils;
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

import static io.datakernel.common.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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
public final class ClassBuilder<T> implements Initializable<ClassBuilder<T>> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static final String DEFAULT_CLASS_NAME = ClassBuilder.class.getPackage().getName() + ".Class";
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private final DefiningClassLoader classLoader;

	private final Class<?> superclass;
	private final List<Class<?>> interfaces;
	private ClassKey key;
	private Path bytecodeSaveDir;
	private final Map<String, Object> staticConstants = new LinkedHashMap<>();

	private String className;

	private final Map<String, Class<?>> fields = new LinkedHashMap<>();
	private final Map<String, Class<?>> staticFields = new LinkedHashMap<>();
	private final Map<Method, Expression> methods = new LinkedHashMap<>();
	private final Map<Method, Expression> staticMethods = new LinkedHashMap<>();

	// region builders

	/**
	 * Creates a new instance of AsmFunctionFactory
	 *
	 * @param classLoader class loader
	 * @param superclass  type of dynamic class
	 */
	private ClassBuilder(DefiningClassLoader classLoader, Class<?> superclass, List<Class<?>> types) {
		this.classLoader = classLoader;
		this.superclass = superclass;
		this.interfaces = types;
		this.key = new ClassKey(superclass, new HashSet<>(interfaces), singletonList(new Object()));
	}

	public static <T> ClassBuilder<T> create(DefiningClassLoader classLoader, Class<? super T> implementation, Class<?>... interfaces) {
		return create(classLoader, implementation, asList(interfaces));
	}

	public static <T> ClassBuilder<T> create(DefiningClassLoader classLoader, Class<? super T> implementation, List<Class<?>> interfaces) {
		checkArgument(interfaces.stream().allMatch(Class::isInterface));
		if (implementation.isInterface()) {
			return new ClassBuilder<>(classLoader,
					Object.class,
					CollectionUtils.concat(singletonList(implementation), interfaces));
		} else {
			return new ClassBuilder<>(classLoader,
					implementation,
					interfaces);
		}
	}

	public ClassBuilder<T> withBytecodeSaveDir(Path bytecodeSaveDir) {
		this.bytecodeSaveDir = bytecodeSaveDir;
		return this;
	}

	public ClassBuilder<T> withClassKey(Object... parameters) {
		this.key = new ClassKey(superclass, new HashSet<>(interfaces), asList(parameters));
		return this;
	}

	public ClassBuilder<T> withClassName(String name) {
		this.className = name;
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

	public ClassBuilder<T> withFields(Map<String, Class<?>> fields) {
		this.fields.putAll(fields);
		return this;
	}

	/**
	 * Creates a new method for a dynamic class
	 *
	 * @param methodName    name of method
	 * @param returnType    type which returns this method
	 * @param argumentTypes list of types of arguments
	 * @param expression    function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public ClassBuilder<T> withMethod(String methodName, Class<?> returnType, List<? extends Class<?>> argumentTypes, Expression expression) {
		methods.put(new Method(methodName, getType(returnType), argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
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
			methods.put(method, expression);
			return this;
		}

		Method foundMethod = null;
		List<List<java.lang.reflect.Method>> listOfMethods = new ArrayList<>();
		listOfMethods.add(asList(Object.class.getMethods()));
		listOfMethods.add(asList(superclass.getMethods()));
		listOfMethods.add(asList(superclass.getDeclaredMethods()));
		for (Class<?> type : interfaces) {
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
		checkArgument(foundMethod != null, "Could not find method '" + methodName + "'");
		methods.put(foundMethod, expression);
		return this;
	}

	public ClassBuilder<T> withStaticMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		staticMethods.put(new Method(methodName, getType(returnClass), argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
		return this;
	}

	public ClassBuilder<T> withStaticField(String fieldName, Class<?> type, Object value) {
		this.staticFields.put(fieldName, type);
		this.staticConstants.put(fieldName, value);
		return this;
	}

	// endregion
	public Class<T> build() {
		synchronized (classLoader) {
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

	private Class<T> defineNewClass(ClassKey key) {
		DefiningClassWriter cw = DefiningClassWriter.create(classLoader);

		String actualClassName;
		if (className == null) {
			actualClassName = DEFAULT_CLASS_NAME + COUNTER.incrementAndGet();
		} else {
			actualClassName = className;
		}

		Type classType = getType('L' + actualClassName.replace('.', '/') + ';');

		cw.visit(V1_6, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
				classType.getInternalName(),
				null,
				getInternalName(superclass),
				interfaces.stream().map(Type::getInternalName).toArray(String[]::new));

		{
			Method m = getMethod("void <init> ()");
			GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);
			g.loadThis();
			g.invokeConstructor(getType(superclass), m);

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

				Context ctx = new Context(classLoader, g, classType, superclass, interfaces, fields, methods, staticMethods, m, staticConstants);

				Expression expression = staticMethods.get(m);
				ctx.cast(expression.load(ctx), m.getReturnType());
				g.returnValue();

				g.endMethod();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		for (Method m : methods.keySet()) {
			try {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);

				Context ctx = new Context(classLoader, g, classType, superclass, interfaces, fields, methods, staticMethods, m, staticConstants);

				Expression expression = methods.get(m);
				ctx.cast(expression.load(ctx), m.getReturnType());
				g.returnValue();

				g.endMethod();
			} catch (RuntimeException e) {
				throw e;
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

		Class<?> definedClass = classLoader.defineClass(key, actualClassName, cw.toByteArray());
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
		Class<?>[] constructorParameterTypes = new Class<?>[constructorParameters.length];
		for (int i = 0; i < constructorParameters.length; i++) {
			constructorParameterTypes[i] = constructorParameters[i].getClass();
		}
		return buildClassAndCreateNewInstance(constructorParameterTypes, constructorParameters);
	}

	public T buildClassAndCreateNewInstance(Class<?>[] constructorParameterTypes, Object[] constructorParameters) {
		try {
			return build().getConstructor(constructorParameterTypes).newInstance(constructorParameters);
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}
}
