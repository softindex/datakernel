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

package io.datakernel.common.reflection;

import io.datakernel.common.exception.UncheckedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.common.Preconditions.checkArgument;

public final class ReflectionUtils {

	public static boolean isPrimitiveType(Class<?> cls) {
		return cls == boolean.class
				|| cls == byte.class
				|| cls == char.class
				|| cls == short.class
				|| cls == int.class
				|| cls == long.class
				|| cls == float.class
				|| cls == double.class;
	}

	public static boolean isBoxedPrimitiveType(Class<?> cls) {
		return cls == Boolean.class
				|| cls == Byte.class
				|| cls == Character.class
				|| cls == Short.class
				|| cls == Integer.class
				|| cls == Long.class
				|| cls == Float.class
				|| cls == Double.class;
	}

	public static boolean isPrimitiveTypeOrBox(Class<?> cls) {
		return isPrimitiveType(cls) || isBoxedPrimitiveType(cls);
	}

	public static boolean isSimpleType(Class<?> cls) {
		return isPrimitiveTypeOrBox(cls) || cls == String.class;
	}

	public static boolean isThrowable(Class<?> cls) {
		return Throwable.class.isAssignableFrom(cls);
	}

	public static boolean isGetter(Method method) {
		return Modifier.isPublic(method.getModifiers())
				&& method.getName().length() > 2
				&& (method.getName().startsWith("get") && method.getReturnType() != void.class
				|| method.getName().startsWith("is") && (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class));
	}

	public static boolean isSetter(Method method) {
		return Modifier.isPublic(method.getModifiers())
				&& method.getName().length() > 3
				&& method.getName().startsWith("set")
				&& method.getReturnType() == void.class
				&& method.getParameterCount() == 1;
	}

	public static String extractFieldNameFromGetter(Method getter) {
		if (getter.getName().startsWith("get")) {
			if (getter.getName().length() == 3) {
				return "";
			}
			String getterName = getter.getName();
			String firstLetter = getterName.substring(3, 4);
			String restOfName = getterName.substring(4);
			return firstLetter.toLowerCase() + restOfName;
		}
		if (getter.getName().startsWith("is") && getter.getName().length() > 2) {
			String getterName = getter.getName();
			String firstLetter = getterName.substring(2, 3);
			String restOfName = getterName.substring(3);
			return firstLetter.toLowerCase() + restOfName;
		}
		throw new IllegalArgumentException("Given method is not a getter");
	}

	public static String extractFieldNameFromSetter(Method setter) {
		String name = setter.getName();
		checkArgument(name.startsWith("set") && name.length() > 3, "Given method is not a setter");

		String firstLetter = name.substring(3, 4);
		String restOfName = name.substring(4);
		return firstLetter.toLowerCase() + restOfName;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private static <T> Supplier<T> getConstructorOrFactory(Class<T> cls, String... factoryMethodNames) {
		for (String methodName : factoryMethodNames) {
			Method method;
			try {
				method = cls.getDeclaredMethod(methodName);
			} catch (NoSuchMethodException e) {
				continue;
			}
			if ((method.getModifiers() & (Modifier.PUBLIC | Modifier.STATIC)) == 0 || method.getReturnType() != cls) {
				continue;
			}
			return () -> {
				try {
					return (T) method.invoke(null);
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw new UncheckedException(e);
				}
			};
		}
		return Arrays.stream(cls.getConstructors())
				.filter(c -> c.getParameterTypes().length == 0)
				.findAny()
				.<Supplier<T>>map(c -> () -> {
					try {
						return (T) c.newInstance();
					} catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
						throw new UncheckedException(e);
					}
				})
				.orElse(null);
	}

	public static boolean canBeCreated(Class<?> cls, String... factoryMethodNames) {
		return getConstructorOrFactory(cls, factoryMethodNames) != null;
	}

	@Nullable
	public static <T> T tryToCreateInstanceWithFactoryMethods(Class<T> cls, String... factoryMethodNames) {
		try {
			Supplier<T> supplier = getConstructorOrFactory(cls, factoryMethodNames);
			return supplier != null ? supplier.get() : null;
		} catch (UncheckedException u) {
			return null;
		}
	}

	public static List<Class<?>> getAllInterfaces(Class<?> cls) {
		Set<Class<?>> interfacesFound = new LinkedHashSet<>();
		getAllInterfaces(cls, interfacesFound);
		return new ArrayList<>(interfacesFound);
	}

	private static void getAllInterfaces(Class<?> cls, Set<Class<?>> found) {
		while (cls != null) {
			if (cls.isInterface()) {
				found.add(cls);
			}
			for (Class<?> interfaceCls : cls.getInterfaces()) {
				if (found.add(interfaceCls)) {
					getAllInterfaces(interfaceCls, found);
				}
			}
			cls = cls.getSuperclass();
		}
	}

	@Nullable
	public static Annotation deepFindAnnotation(Class<?> cls, Predicate<Annotation> predicate) {
		return doDeepFindAnnotation(cls, predicate, new HashSet<>());
	}

	@Nullable
	private static Annotation doDeepFindAnnotation(Class<?> cls, Predicate<Annotation> predicate, Set<Class<?>> visited) {
		if (cls == Object.class || !visited.add(cls)) return null;
		for (Annotation annotation : cls.getAnnotations()) {
			if (predicate.test(annotation)){
				return annotation;
			}
		}
		for (Class<?> anInterface : cls.getInterfaces()) {
			Annotation annotation = doDeepFindAnnotation(anInterface, predicate, visited);
			if (annotation != null) return annotation;
		}
		Class<?> superclass = cls.getSuperclass();
		if (superclass != null) {
			return doDeepFindAnnotation(superclass, predicate, visited);
		}
		return null;
	}

	public static String getAnnotationString(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) throws ReflectiveOperationException {
		if (annotation != null) {
			return getAnnotationString(annotation);
		}
		return annotationType.getSimpleName();
	}

	/**
	 * Builds string representation of annotation with its elements.
	 * The string looks differently depending on the number of elements, that an annotation has.
	 * If annotation has no elements, string looks like this : "AnnotationName"
	 * If annotation has a single element with the name "value", string looks like this : "AnnotationName(someValue)"
	 * If annotation has one or more custom elements, string looks like this : "(key1=value1,key2=value2)"
	 */
	public static String getAnnotationString(Annotation annotation) throws ReflectiveOperationException {
		Class<? extends Annotation> annotationType = annotation.annotationType();
		StringBuilder annotationString = new StringBuilder();
		Method[] annotationElements = filterNonEmptyElements(annotation);
		if (annotationElements.length == 0) {
			// annotation without elements
			annotationString.append(annotationType.getSimpleName());
			return annotationString.toString();
		}
		if (annotationElements.length == 1 && annotationElements[0].getName().equals("value")) {
			// annotation with single element which has name "value"
			annotationString.append(annotationType.getSimpleName());
			Object value = fetchAnnotationElementValue(annotation, annotationElements[0]);
			annotationString.append('(').append(value.toString()).append(')');
			return annotationString.toString();
		}
		// annotation with one or more custom elements
		annotationString.append('(');
		for (Method annotationParameter : annotationElements) {
			Object value = fetchAnnotationElementValue(annotation, annotationParameter);
			String nameKey = annotationParameter.getName();
			String nameValue = value.toString();
			annotationString.append(nameKey).append('=').append(nameValue).append(',');
		}

		assert annotationString.substring(annotationString.length() - 1).equals(",");

		annotationString = new StringBuilder(annotationString.substring(0, annotationString.length() - 1));
		annotationString.append(')');
		return annotationString.toString();
	}

	public static Method[] filterNonEmptyElements(Annotation annotation) throws ReflectiveOperationException {
		List<Method> filtered = new ArrayList<>();
		for (Method method : annotation.annotationType().getDeclaredMethods()) {
			Object elementValue = fetchAnnotationElementValue(annotation, method);
			if (elementValue instanceof String) {
				String stringValue = (String) elementValue;
				if (stringValue.length() == 0) {
					// skip this element, because it is empty string
					continue;
				}
			}
			filtered.add(method);
		}
		return filtered.toArray(new Method[0]);
	}

	/**
	 * Returns values if it is not null, otherwise throws exception
	 */
	public static Object fetchAnnotationElementValue(Annotation annotation, Method element) throws ReflectiveOperationException {
		Object value = element.invoke(annotation);
		if (value == null) {
			String errorMsg = "@" + annotation.annotationType().getName() + "." +
					element.getName() + "() returned null";
			throw new NullPointerException(errorMsg);
		}
		return value;
	}

	public static boolean isPrivateApiAvailable() {
		try {
			Class.forName("java.lang.Module");
			return false;
		} catch (ClassNotFoundException e) {
			return true;
		}
	}
}
