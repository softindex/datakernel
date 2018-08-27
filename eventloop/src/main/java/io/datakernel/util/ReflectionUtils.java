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

package io.datakernel.util;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.Preconditions.checkArgument;

public final class ReflectionUtils {

	private ReflectionUtils() {
	}

	public static boolean isJmxStats(Class<?> clazz) {
		return JmxStats.class.isAssignableFrom(clazz);
	}

	public static boolean isJmxRefreshableStats(Class<?> clazz) {
		return JmxRefreshableStats.class.isAssignableFrom(clazz);
	}

	public static boolean isPrimitiveType(Class<?> clazz) {
		return boolean.class.isAssignableFrom(clazz)
				|| byte.class.isAssignableFrom(clazz)
				|| short.class.isAssignableFrom(clazz)
				|| char.class.isAssignableFrom(clazz)
				|| int.class.isAssignableFrom(clazz)
				|| long.class.isAssignableFrom(clazz)
				|| float.class.isAssignableFrom(clazz)
				|| double.class.isAssignableFrom(clazz);
	}

	public static boolean isPrimitiveTypeWrapper(Class<?> clazz) {
		return Boolean.class.isAssignableFrom(clazz)
				|| Byte.class.isAssignableFrom(clazz)
				|| Short.class.isAssignableFrom(clazz)
				|| Character.class.isAssignableFrom(clazz)
				|| Integer.class.isAssignableFrom(clazz)
				|| Long.class.isAssignableFrom(clazz)
				|| Float.class.isAssignableFrom(clazz)
				|| Double.class.isAssignableFrom(clazz);
	}

	public static boolean isString(Class<?> clazz) {
		return String.class.isAssignableFrom(clazz);
	}

	public static boolean isThrowable(Class<?> clazz) {
		return Throwable.class.isAssignableFrom(clazz);
	}

	public static boolean isGetter(Method method) {
		boolean returnsBoolean = method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class;
		boolean isIsGetter = method.getName().length() > 2 && method.getName().startsWith("is") && returnsBoolean;

		boolean doesntReturnVoid = method.getReturnType() != void.class;
		boolean isGetGetter = method.getName().length() >= 3 && method.getName().startsWith("get") && doesntReturnVoid;

		return isIsGetter || isGetGetter;
	}

	public static String extractFieldNameFromGetter(Method getter) {
		checkArgument(isGetter(getter));

		if (getter.getName().startsWith("get")) {
			if (getter.getName().length() == 3) {
				return "";
			} else {
				String getterName = getter.getName();
				String firstLetter = getterName.substring(3, 4);
				String restOfName = getterName.substring(4);
				return firstLetter.toLowerCase() + restOfName;
			}
		} else if (getter.getName().startsWith("is")) {
			String getterName = getter.getName();
			String firstLetter = getterName.substring(2, 3);
			String restOfName = getterName.substring(3);
			return firstLetter.toLowerCase() + restOfName;
		} else {
			throw new RuntimeException();
		}
	}

	public static boolean isSetter(Method method) {
		boolean hasSingleParameter = method.getParameterTypes().length == 1;
		return method.getName().length() > 3 && method.getName().startsWith("set")
				&& method.getReturnType() == void.class && hasSingleParameter;
	}

	public static String extractFieldNameFromSetter(Method setter) {
		checkArgument(isSetter(setter));

		String setterName = setter.getName();
		String firstLetter = setterName.substring(3, 4);
		String restOfName = setterName.substring(4);
		return firstLetter.toLowerCase() + restOfName;
	}

	public static boolean classHasPublicNoArgConstructor(Class<?> clazz) {
		for (Constructor<?> constructor : clazz.getConstructors()) {
			if (constructor.getParameterTypes().length == 0) {
				return true;
			}
		}
		return false;
	}

	public static boolean classHasPublicStaticFactoryMethod(Class<?> clazz, String methodName) {
		Method createMethod;
		try {
			createMethod = clazz.getDeclaredMethod(methodName);
		} catch (NoSuchMethodException e) {
			return false;
		}

		return Modifier.isStatic(createMethod.getModifiers()) &&
				Modifier.isPublic(createMethod.getModifiers()) &&
				createMethod.getReturnType().equals(clazz);
	}

	private static void visitFields(Object instance, Function<Object, Boolean> action) {
		if (instance == null) return;
		for (Method method : instance.getClass().getMethods()) {
			if (method.getParameters().length != 0 || !Modifier.isPublic(method.getModifiers()))
				continue;
			Class<?> returnType = method.getReturnType();
			if (returnType == void.class || returnType == String.class || isPrimitiveType(returnType) || isPrimitiveTypeWrapper(returnType))
				continue;
			if (Arrays.stream(method.getAnnotations()).noneMatch(a -> a.annotationType() == JmxAttribute.class))
				continue;
			Object fieldValue;
			try {
				fieldValue = method.invoke(instance);
			} catch (IllegalAccessException | InvocationTargetException e) {
				continue;
			}
			if (fieldValue == null)
				continue;
			if (action.apply(fieldValue)) continue;
			if (Map.class.isAssignableFrom(returnType)) {
				for (Object item : ((Map<?, ?>) fieldValue).values()) {
					visitFields(item, action);
				}
			} else if (Collection.class.isAssignableFrom(returnType)) {
				for (Object item : ((Collection<?>) fieldValue)) {
					visitFields(item, action);
				}
			} else {
				visitFields(fieldValue, action);
			}
		}
	}

	public static void resetStats(Object instance) {
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithReset) {
				((JmxStatsWithReset) item).resetStats();
				return true;
			}
			return false;
		});
	}

	public static void setSmoothingWindow(Object instance, Duration smoothingWindowSeconds) {
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithSmoothingWindow) {
				((JmxStatsWithSmoothingWindow) item).setSmoothingWindow(smoothingWindowSeconds);
				return true;
			}
			return false;
		});
	}

	@Nullable
	public static Duration getSmoothingWindow(Object instance) {
		Set<Duration> result = new HashSet<>();
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithSmoothingWindow) {
				Duration smoothingWindow = ((JmxStatsWithSmoothingWindow) item).getSmoothingWindow();
				result.add(smoothingWindow);
				return true;
			}
			return false;
		});
		if (result.size() == 1) {
			return first(result);
		}
		return null;
	}

	/**
	 * Builds string representation of annotation with its elements.
	 * The string looks differently depending on the number of elements, that an annotation has.
	 * If annotation has no elements, string looks like this : "AnnotationName"
	 * If annotation has a single element with the name "value", string looks like this : "AnnotationName(someValue)"
	 * If annotation has one or more custom elements, string looks like this : "(key1=value1,key2=value2)"
	 * @param annotation
	 * @return String representation of annotation with its elements
	 * @throws ReflectiveOperationException
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

}
