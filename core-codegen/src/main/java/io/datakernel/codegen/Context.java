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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.codegen.Utils.*;
import static io.datakernel.common.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;
import static org.objectweb.asm.Type.VOID_TYPE;
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

	public Class<?> toJavaType(Type type) {
		if (type.equals(getSelfType()))
			throw new IllegalArgumentException();
		int sort = type.getSort();
		if (sort == Type.BOOLEAN)
			return boolean.class;
		if (sort == Type.CHAR)
			return char.class;
		if (sort == Type.BYTE)
			return byte.class;
		if (sort == Type.SHORT)
			return short.class;
		if (sort == Type.INT)
			return int.class;
		if (sort == Type.FLOAT)
			return float.class;
		if (sort == Type.LONG)
			return long.class;
		if (sort == Type.DOUBLE)
			return double.class;
		if (sort == Type.VOID)
			return void.class;
		if (sort == Type.OBJECT) {
			try {
				return classLoader.loadClass(type.getClassName());
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(format("No class %s in class loader", type.getClassName()), e);
			}
		}
		if (sort == Type.ARRAY) {
			Class<?> result;
			if (type.equals(getType(Object[].class))) {
				result = Object[].class;
			} else {
				String className = type.getDescriptor().replace('/', '.');
				try {
					result = Class.forName(className);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException(format("No class %s in Class.forName", className), e);
				}
			}
			return result;
		}
		throw new IllegalArgumentException(format("No Java type for %s", type.getClassName()));
	}

	public void cast(Type type, Type targetType) {
		GeneratorAdapter g = getGeneratorAdapter();

		if (type.equals(targetType)) {
			return;
		}

		if (targetType == VOID_TYPE) {
			if (type.getSize() == 1)
				g.pop();
			if (type.getSize() == 2)
				g.pop2();
			return;
		}

		if (type == VOID_TYPE) {
			throw new RuntimeException(format("Can't cast VOID_TYPE to %s. %s",
					targetType.getClassName(),
					exceptionInGeneratedClass(this)));
		}

		if (type.equals(getSelfType())) {
			Class<?> javaType = toJavaType(targetType);
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
				toJavaType(targetType).isAssignableFrom(toJavaType(type))) {
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

	public Type invoke(Expression owner, String methodName, Expression... arguments) {
		return invoke(owner, methodName, asList(arguments));
	}

	public Type invoke(Expression owner, String methodName, List<Expression> arguments) {
		Type ownerType = owner.load(this);
		Type[] argumentTypes = new Type[arguments.size()];
		for (int i = 0; i < arguments.size(); i++) {
			Expression argument = arguments.get(i);
			argumentTypes[i] = argument.load(this);
		}
		return invoke(ownerType, methodName, argumentTypes);
	}

	public Type invoke(Type ownerType, String methodName, Type... argumentTypes) {
		Class<?>[] arguments = Stream.of(argumentTypes).map(this::toJavaType).toArray(Class[]::new);
		Method foundMethod;
		if (ownerType.equals(getSelfType())) {
			foundMethod = findMethod(
					getMethods().keySet().stream(),
					methodName,
					arguments);
			g.invokeVirtual(ownerType, foundMethod);
		} else {
			Class<?> javaOwnerType = toJavaType(ownerType);
			foundMethod = findMethod(
					Arrays.stream(javaOwnerType.getMethods())
							.filter(m -> !isStatic(m.getModifiers()))
							.map(m -> new Method(m.getName(),
									getType(m.getReturnType()),
									Arrays.stream(m.getParameterTypes()).map(Type::getType).toArray(Type[]::new))),
					methodName,
					arguments);
			if (javaOwnerType.isInterface()) {
				g.invokeInterface(ownerType, foundMethod);
			} else {
				g.invokeVirtual(ownerType, foundMethod);
			}
		}
		return foundMethod.getReturnType();
	}

	public Type invokeStatic(Type ownerType, String methodName, Expression... arguments) {
		return invokeStatic(ownerType, methodName, asList(arguments));
	}

	public Type invokeStatic(Type ownerType, String methodName, List<Expression> arguments) {
		Type[] argumentTypes = new Type[arguments.size()];
		for (int i = 0; i < arguments.size(); i++) {
			Expression argument = arguments.get(i);
			argumentTypes[i] = argument.load(this);
		}
		return invokeStatic(ownerType, methodName, argumentTypes);
	}

	public Type invokeStatic(Type ownerType, String methodName, Type... argumentTypes) {
		Class<?>[] arguments = Stream.of(argumentTypes).map(this::toJavaType).toArray(Class[]::new);
		Method foundMethod;
		if (ownerType.equals(getSelfType())) {
			foundMethod = findMethod(
					getStaticMethods().keySet().stream(),
					methodName,
					arguments);
		} else {
			foundMethod = findMethod(
					Arrays.stream(toJavaType(ownerType).getMethods())
							.filter(m -> isStatic(m.getModifiers()))
							.map(m -> new Method(m.getName(),
									getType(m.getReturnType()),
									Arrays.stream(m.getParameterTypes()).map(Type::getType).toArray(Type[]::new))),
					methodName,
					arguments);
		}
		g.invokeStatic(ownerType, foundMethod);
		return foundMethod.getReturnType();
	}

	public Type invokeConstructor(Type ownerType, Expression... arguments) {
		return invokeConstructor(ownerType, asList(arguments));
	}

	public Type invokeConstructor(Type ownerType, List<Expression> arguments) {
		g.newInstance(ownerType);
		g.dup();

		Type[] argumentTypes = new Type[arguments.size()];
		for (int i = 0; i < arguments.size(); i++) {
			argumentTypes[i] = arguments.get(i).load(this);
		}
		return invokeConstructor(ownerType, argumentTypes);
	}

	public Type invokeConstructor(Type ownerType, Type... argumentTypes) {
		Class<?>[] arguments = Stream.of(argumentTypes).map(this::toJavaType).toArray(Class[]::new);
		checkArgument(!ownerType.equals(getSelfType()));
		Method foundMethod = findMethod(
				Arrays.stream(toJavaType(ownerType).getConstructors())
						.map(m -> new Method("<init>", VOID_TYPE,
								Arrays.stream(m.getParameterTypes()).map(Type::getType).toArray(Type[]::new))),
				"<init>",
				arguments);
		g.invokeConstructor(ownerType, foundMethod);
		return ownerType;
	}

	private Method findMethod(Stream<Method> methods, String name, Class<?>[] arguments) {
		Method foundMethod = null;
		Class<?>[] foundMethodArguments = null;

		for (Iterator<Method> it = methods.iterator(); it.hasNext(); ) {
			Method method = it.next();
			if (!name.equals(method.getName())) continue;
			Class[] methodArguments = Stream.of(method.getArgumentTypes()).map(this::toJavaType).toArray(Class[]::new);
			if (!isAssignable(methodArguments, arguments)) {
				continue;
			}
			if (foundMethod == null) {
				foundMethod = method;
				foundMethodArguments = methodArguments;
			} else {
				if (isAssignable(foundMethodArguments, methodArguments)) {
					foundMethod = method;
					foundMethodArguments = methodArguments;
				} else if (isAssignable(methodArguments, foundMethodArguments)) {
					// do nothing
				} else {
					throw new IllegalArgumentException("Ambiguous method: " + method + " " + Arrays.toString(arguments));
				}
			}
		}

		if (foundMethod == null) {
			throw new IllegalArgumentException("Method not found: " + name + " " + Arrays.toString(arguments));
		}

		return foundMethod;
	}

	private static boolean isAssignable(Class<?>[] to, Class<?>[] from) {
		if (to.length != from.length) return false;
		return IntStream.range(0, from.length)
				.allMatch(i -> to[i].isAssignableFrom(from[i]));
	}

}
