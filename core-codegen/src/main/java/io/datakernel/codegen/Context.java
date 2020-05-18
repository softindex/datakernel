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

import io.datakernel.codegen.expression.Expression;
import io.datakernel.codegen.expression.VarLocal;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.codegen.util.Utils.*;
import static io.datakernel.common.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.objectweb.asm.Type.*;

/**
 * Contains information about a dynamic class
 */
public final class Context {
	private final DefiningClassLoader classLoader;
	private final GeneratorAdapter g;
	private final Type selfType;
	private final Class<?> superclass;
	private final List<Class<?>> interfaces;
	private final Map<String, Class<?>> fields;
	private final Map<Method, Expression> methods;
	private final Map<Method, Expression> staticMethods;
	private final Method method;
	private final Map<String, Object> staticConstants;

	public Context(DefiningClassLoader classLoader,
			GeneratorAdapter g,
			Type selfType,
			Class<?> superclass,
			List<Class<?>> interfaces,
			Map<String, Class<?>> fields,
			Map<Method, Expression> methods,
			Map<Method, Expression> staticMethods,
			Method method,
			Map<String, Object> staticConstants) {
		this.classLoader = classLoader;
		this.g = g;
		this.selfType = selfType;
		this.superclass = superclass;
		this.interfaces = interfaces;
		this.fields = fields;
		this.methods = methods;
		this.staticMethods = staticMethods;
		this.method = method;
		this.staticConstants = staticConstants;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	public GeneratorAdapter getGeneratorAdapter() {
		return g;
	}

	public Type getSelfType() {
		return selfType;
	}

	public Class<?> getSuperclass() {
		return superclass;
	}

	public List<Class<?>> getInterfaces() {
		return interfaces;
	}

	public Map<String, Class<?>> getFields() {
		return fields;
	}

	public Map<Method, Expression> getMethods() {
		return methods;
	}

	public Map<Method, Expression> getStaticMethods() {
		return staticMethods;
	}

	public Method getMethod() {
		return method;
	}

	public Map<String, Object> getStaticConstants() {
		return staticConstants;
	}

	public void addStaticConstant(String field, Object value) {
		staticConstants.put(field, value);
	}

	public VarLocal newLocal(Type type) {
		int local = getGeneratorAdapter().newLocal(type);
		return new VarLocal(local);
	}

	public Class<?> toJavaType(Type type) {
		if (type.equals(getSelfType()))
			throw new IllegalArgumentException();
		int sort = type.getSort();
		if (sort == BOOLEAN)
			return boolean.class;
		if (sort == CHAR)
			return char.class;
		if (sort == BYTE)
			return byte.class;
		if (sort == SHORT)
			return short.class;
		if (sort == INT)
			return int.class;
		if (sort == FLOAT)
			return float.class;
		if (sort == LONG)
			return long.class;
		if (sort == DOUBLE)
			return double.class;
		if (sort == VOID)
			return void.class;
		if (sort == OBJECT) {
			try {
				return classLoader.loadClass(type.getClassName());
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(format("No class %s in class loader", type.getClassName()), e);
			}
		}
		if (sort == ARRAY) {
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

	public void cast(Type typeFrom, Type typeTo) {
		GeneratorAdapter g = getGeneratorAdapter();

		if (typeFrom.equals(typeTo)) {
			return;
		}

		if (typeTo == VOID_TYPE) {
			if (typeFrom.getSize() == 1)
				g.pop();
			if (typeFrom.getSize() == 2)
				g.pop2();
			return;
		}

		if (typeFrom == VOID_TYPE) {
			throw new RuntimeException(format("Can't cast VOID_TYPE typeTo %s. %s",
					typeTo.getClassName(),
					exceptionInGeneratedClass(this)));
		}

		if (typeFrom.equals(getSelfType())) {
			Class<?> javaType = toJavaType(typeTo);
			if (javaType.isAssignableFrom(getSuperclass())) {
				return;
			}
			for (Class<?> type : getInterfaces()) {
				if (javaType.isAssignableFrom(type)) {
					return;
				}
			}
			throw new RuntimeException(format("Can't cast self %s typeTo %s, %s",
					typeFrom.getClassName(),
					typeTo.getClassName(),
					exceptionInGeneratedClass(this)));
		}

		if (!typeFrom.equals(getSelfType()) && !typeTo.equals(getSelfType()) &&
				toJavaType(typeTo).isAssignableFrom(toJavaType(typeFrom))) {
			return;
		}

		if (typeTo.equals(getType(Object.class)) && isPrimitiveType(typeFrom)) {
			g.box(typeFrom);
//			g.cast(wrap(typeFrom), getType(Object.class));
			return;
		}

		if ((isPrimitiveType(typeFrom) || isWrapperType(typeFrom)) &&
				(isPrimitiveType(typeTo) || isWrapperType(typeTo))) {

			Type targetTypePrimitive = isPrimitiveType(typeTo) ? typeTo : unwrap(typeTo);

			if (isWrapperType(typeFrom)) {
				g.invokeVirtual(typeFrom, toPrimitive(typeTo));
				return;
			}

			assert isPrimitiveType(typeFrom);

			if (isValidCast(typeFrom, targetTypePrimitive)) {
				g.cast(typeFrom, targetTypePrimitive);
			}

			if (isWrapperType(typeTo)) {
				g.valueOf(targetTypePrimitive);
			}

			return;
		}

		g.checkCast(typeTo);
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
							.map(Method::getMethod),
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
							.map(Method::getMethod),
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
						.map(Method::getMethod),
				"<init>",
				arguments);
		g.invokeConstructor(ownerType, foundMethod);
		return ownerType;
	}

	private Method findMethod(Stream<Method> methods, String name, Class<?>[] arguments) {
		Set<Method> methodSet = methods.collect(toSet());

		methodSet.addAll(Arrays.stream(Object.class.getMethods())
				.filter(m -> !isStatic(m.getModifiers()))
				.map(Method::getMethod)
				.collect(toSet()));

		Method foundMethod = null;
		Class<?>[] foundMethodArguments = null;

		for (Method method : methodSet) {
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
