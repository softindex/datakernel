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

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.codegen.Utils.*;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.objectweb.asm.Type.getType;

/**
 * Contains information about a dynamic class
 */
public final class Context {
	private final DefiningClassLoader classLoader;
	private final GeneratorAdapter g;
	private final Type selfType;
	private final Method method;
	private final Class<?> mainClass;
	private final List<Class<?>> otherClasses;
	private final Map<String, Class<?>> fields;
	private final Map<String, Object> staticConstants;
	private final Type[] argumentTypes;
	private final Map<Method, Expression> methods;
	private final Map<Method, Expression> staticMethods;

	public Context(DefiningClassLoader classLoader, GeneratorAdapter g,
			Type selfType,
			Class<?> mainClass,
			List<Class<?>> otherClasses,
			Map<String, Class<?>> fields,
			Map<String, Object> staticConstants,
			Type[] argumentTypes,
			Method method,
			Map<Method, Expression> methods,
			Map<Method, Expression> staticMethods) {
		this.classLoader = classLoader;
		this.g = g;
		this.method = method;
		this.mainClass = mainClass;
		this.otherClasses = otherClasses;
		this.argumentTypes = argumentTypes;
		this.selfType = selfType;
		this.fields = fields;
		this.staticConstants = staticConstants;
		this.methods = methods;
		this.staticMethods = staticMethods;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	public GeneratorAdapter getGeneratorAdapter() {
		return g;
	}

	public Class<?> getMainClass() {
		return mainClass;
	}

	public List<Class<?>> getOtherClasses() {
		return otherClasses;
	}

	public Type getSelfType() {
		return selfType;
	}

	public Map<String, Class<?>> getFields() {
		return fields;
	}

	public Map<String, Object> getStaticConstants() {
		return staticConstants;
	}

	public void addStaticConstant(String field, Object value) {
		staticConstants.put(field, value);
	}

	public Type[] getArgumentTypes() {
		return argumentTypes;
	}

	public Type getArgumentType(int argument) {
		return argumentTypes[argument];
	}

	public Map<Method, Expression> getStaticMethods() {
		return staticMethods;
	}

	public Map<Method, Expression> getMethods() {
		return methods;
	}

	public Method getMethod() {
		return method;
	}

	public Type invoke(Expression owner, String methodName, Expression... arguments) {
		Type ownerType = owner.load(this);
		Type[] argumentTypes = new Type[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			Expression argument = arguments[i];
			argumentTypes[i] = argument.load(this);
		}
		return invoke(ownerType, methodName, argumentTypes);
	}

	public Type invoke(Type ownerType, String methodName, Type... argumentTypes) {
		Class<?>[] argumentClasses = Stream.of(argumentTypes).map(type -> getJavaType(getClassLoader(), type)).toArray(Class[]::new);
		try {
			if (!getSelfType().equals(ownerType)) {
				Class<?> javaOwnerType = getJavaType(getClassLoader(), ownerType);
				java.lang.reflect.Method javaMethod = javaOwnerType.getMethod(methodName, argumentClasses);
				Type returnType = getType(javaMethod.getReturnType());
				invokeVirtualOrInterface(g, javaOwnerType, new org.objectweb.asm.commons.Method(methodName, returnType, argumentTypes));
				return returnType;
			}
			for (Method method : getMethods().keySet()) {
				if (method.getName().equals(methodName) && method.getArgumentTypes().length == argumentTypes.length) {
					Type[] methodTypes = method.getArgumentTypes();
					if (IntStream.range(0, argumentTypes.length).allMatch(i -> methodTypes[i].equals(argumentTypes[i]))) {
						g.invokeVirtual(ownerType, method);
						return method.getReturnType();
					}
				}
			}
			throw new NoSuchMethodException();
		} catch (NoSuchMethodException ignored) {
			throw new IllegalArgumentException(
					format("No method %s.%s(%s). %s",
							ownerType.getClassName(),
							methodName,
							Arrays.stream(argumentClasses).map(Objects::toString).collect(joining(",")),
							exceptionInGeneratedClass(this)));
		}
	}

	public void cast(Type type, Type targetType) {
		GeneratorAdapter g = getGeneratorAdapter();

		if (type.equals(targetType)) {
			return;
		}

		if (targetType == Type.VOID_TYPE) {
			if (type.getSize() == 1)
				g.pop();
			if (type.getSize() == 2)
				g.pop2();
			return;
		}

		if (type == Type.VOID_TYPE) {
			throw new RuntimeException(format("Can't cast VOID_TYPE to %s. %s",
					targetType.getClassName(),
					exceptionInGeneratedClass(this)));
		}

		if (type.equals(getSelfType())) {
			Class<?> javaType = getJavaType(getClassLoader(), targetType);
			if (javaType.isAssignableFrom(getMainClass())) {
				return;
			}
			for (Class<?> aClass : getOtherClasses()) {
				if (javaType.isAssignableFrom(aClass)) {
					return;
				}
			}
			throw new RuntimeException(format("Can't cast self %s to %s, %s",
					type.getClassName(),
					targetType.getClassName(),
					exceptionInGeneratedClass(this)));
		}

		if (!type.equals(getSelfType()) && !targetType.equals(getSelfType()) &&
				getJavaType(getClassLoader(), targetType).isAssignableFrom(getJavaType(getClassLoader(), type))) {
			return;
		}

		if (targetType.equals(getType(Object.class)) && isPrimitiveType(type)) {
			g.box(type);
//			g.cast(wrap(type), getType(Object.class));
			return;
		}

		if ((isPrimitiveType(type) || isWrapperType(type)) &&
				(isPrimitiveType(targetType) || isWrapperType(targetType))) {

			Type targetTypePrimitive = isPrimitiveType(targetType) ? targetType : unwrap(targetType);

			if (isWrapperType(type)) {
				g.invokeVirtual(type, primitiveValueMethod(targetType));
				return;
			}

			assert isPrimitiveType(type);

			if (isValidCast(type, targetTypePrimitive)) {
				g.cast(type, targetTypePrimitive);
			}

			if (isWrapperType(targetType)) {
				g.valueOf(targetTypePrimitive);
			}

			return;
		}

		g.checkCast(targetType);
	}

}
