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

import io.datakernel.exception.UncheckedException;
import io.datakernel.jmx.*;
import org.jetbrains.annotations.Nullable;

import javax.management.MXBean;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.stream.Collectors.toSet;

public final class ReflectionUtils {

	private ReflectionUtils() {
	}

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

	public static boolean isJmxStats(Class<?> cls) {
		return JmxStats.class.isAssignableFrom(cls);
	}

	public static boolean isJmxRefreshableStats(Class<?> cls) {
		return JmxRefreshableStats.class.isAssignableFrom(cls);
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

	private static void visitFields(Object instance, Predicate<Object> action) {
		if (instance == null) {
			return;
		}
		for (Method method : instance.getClass().getMethods()) {
			if (method.getParameters().length != 0 || !Modifier.isPublic(method.getModifiers()))
				continue;
			Class<?> returnType = method.getReturnType();
			if (returnType == void.class || isSimpleType(returnType))
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
			if (action.test(fieldValue)) continue;
			if (Map.class.isAssignableFrom(returnType)) {
				for (Object item : ((Map<?, ?>) fieldValue).values()) {
					visitFields(item, action);
				}
			} else if (Collection.class.isAssignableFrom(returnType)) {
				for (Object item : (Collection<?>) fieldValue) {
					visitFields(item, action);
				}
			} else {
				visitFields(fieldValue, action);
			}
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

	private static boolean isBeanInterface(Class<?> cls) {
		String name = cls.getName();
		return name.endsWith("MBean") || name.endsWith("MXBean") || cls.isAnnotationPresent(MXBean.class);
	}

	public static boolean isBean(Class<?> cls) {
		return getAllInterfaces(cls).stream().anyMatch(ReflectionUtils::isBeanInterface);
	}

	public static Map<String, Object> getJmxAttributes(@Nullable Object instance) {
		Map<String, Object> result = new LinkedHashMap<>();
		getJmxAttributes(instance, "", result);
		return result;
	}

	private static boolean getJmxAttributes(@Nullable Object instance, String rootName, Map<String, Object> attrs) {
		if (instance == null) {
			return false;
		}
		Set<String> attributeNames = getAllInterfaces(instance.getClass()).stream()
			.filter(ReflectionUtils::isBeanInterface)
			.flatMap(i -> Arrays.stream(i.getMethods())
				.filter(ReflectionUtils::isGetter)
				.map(Method::getName))
			.collect(toSet());
		boolean[] changed = {false};
		Arrays.stream(instance.getClass().getMethods())
			.filter(method -> isGetter(method) && (attributeNames.contains(method.getName())
				|| Arrays.stream(method.getAnnotations()).anyMatch(a -> a.annotationType() == JmxAttribute.class)))
			.sorted(Comparator.comparing(Method::getName))
			.forEach(method -> {
				Object fieldValue;
				method.setAccessible(true); // anonymous classes do not work without this, wow
				try {
					fieldValue = method.invoke(instance);
				} catch (IllegalAccessException | InvocationTargetException e) {
					return;
				}
				changed[0] = true;
				JmxAttribute annotation = method.getAnnotation(JmxAttribute.class);
				String name = annotation == null || annotation.name().equals(JmxAttribute.USE_GETTER_NAME) ?
					extractFieldNameFromGetter(method) :
					annotation.name();
				name = rootName.isEmpty() ? name : rootName + (name.isEmpty() ? "" : "_" + name);
				if (fieldValue != instance && !attrs.containsKey(name) && !getJmxAttributes(fieldValue, name, attrs)) {
					attrs.put(name, fieldValue);
				}
			});
		return changed[0];
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
