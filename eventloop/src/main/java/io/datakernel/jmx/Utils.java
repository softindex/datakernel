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

package io.datakernel.jmx;

import javax.management.openmbean.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.*;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class Utils {

	private Utils() {}

	private static final CompositeType DEFAULT_COMPOSITE_TYPE;
	private static final TabularType DEFAULT_TABULAR_TYPE;

	private static final Map<Class<?>, SimpleType<?>> clazzToSimpleType;

	static {
		clazzToSimpleType = new HashMap<>();
		clazzToSimpleType.put(Boolean.class, SimpleType.BOOLEAN);
		clazzToSimpleType.put(Byte.class, SimpleType.BYTE);
		clazzToSimpleType.put(Short.class, SimpleType.SHORT);
		clazzToSimpleType.put(Character.class, SimpleType.CHARACTER);
		clazzToSimpleType.put(Integer.class, SimpleType.INTEGER);
		clazzToSimpleType.put(Long.class, SimpleType.LONG);
		clazzToSimpleType.put(Float.class, SimpleType.FLOAT);
		clazzToSimpleType.put(Double.class, SimpleType.DOUBLE);
		clazzToSimpleType.put(String.class, SimpleType.STRING);

		try {
			DEFAULT_COMPOSITE_TYPE = new CompositeType("EmptyCompositeData", "EmptyCompositeData",
					new String[]{"key"}, new String[]{"key"}, new OpenType<?>[]{SimpleType.STRING});
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}

		try {
			DEFAULT_TABULAR_TYPE = new TabularType("EmptyTabularData", "EmptyTabularData",
					DEFAULT_COMPOSITE_TYPE, new String[]{"key"});
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

	public static String extractFieldNameFromGetter(Method method) {
		checkArgument(isGetter(method));

		String getterName = method.getName();
		String firstLetter = getterName.substring(3, 4);
		String restOfName = getterName.substring(4);
		return firstLetter.toLowerCase() + restOfName;
	}

	public static boolean isGetterOfJmxStats(Method method) {
		boolean returnsJmxStats = JmxStats.class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsJmxStats && hasNoArgs;
	}

	public static boolean isJmxStats(Class<?> clazz) {
		return JmxStats.class.isAssignableFrom(clazz);
	}

	public static boolean isGetterOfSimpleType(Method method) {
		boolean returnsSimpleType = boolean.class.isAssignableFrom(method.getReturnType())
				|| int.class.isAssignableFrom(method.getReturnType())
				|| long.class.isAssignableFrom(method.getReturnType())
				|| double.class.isAssignableFrom(method.getReturnType())
				|| String.class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		// TODO(vmykhalko): maybe also condisder "is*" getter for boolean instead of "get*" getter ?
		return isGetter(method) && returnsSimpleType && hasNoArgs;
	}

	public static boolean isGetterOfPrimitiveType(Method method) {
		boolean returnsPrimitive = isPrimitiveType(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsPrimitive && hasNoArgs;
	}

	public static boolean isGetterOfPrimitiveTypeWrapper(Method method) {
		boolean returnsPrimitive = isPrimitiveTypeWrapper(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsPrimitive && hasNoArgs;
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

	public static boolean isList(Class<?> clazz) {
		return List.class.isAssignableFrom(clazz);
	}

	public static boolean isArray(Class<?> clazz) {
		return Object[].class.isAssignableFrom(clazz);
	}

	public static boolean isMap(Class<?> clazz) {
		return Map.class.isAssignableFrom(clazz);
	}

	public static boolean isThrowable(Class<?> clazz) {
		return Throwable.class.isAssignableFrom(clazz);
	}

	public static boolean isGetterOfString(Method method) {
		boolean returnsString = String.class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsString && hasNoArgs;
	}

	public static boolean isGetterOfList(Method method) {
		boolean returnsList = List.class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsList && hasNoArgs;
	}

	public static boolean isGetterOfArray(Method method) {
		boolean returnsArray = Object[].class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsArray && hasNoArgs;
	}

	public static boolean isGetterOfMap(Method method) {
		boolean returnsMap = Map.class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsMap && hasNoArgs;
	}

	public static boolean isGetterOfThrowable(Method method) {
		boolean returnsThrowable = Throwable.class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		return isGetter(method) && returnsThrowable && hasNoArgs;
	}

	public static boolean isGetterOfPojo(Method method) {
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		boolean doesNotReturnStandardType =
				!isGetterOfPrimitiveType(method) &&
						!isGetterOfPrimitiveTypeWrapper(method) &&
						!isGetterOfString(method) &&
						!isGetterOfArray(method) &&
						!isGetterOfList(method) &&
						!isGetterOfThrowable(method);
		return isGetter(method) && doesNotReturnStandardType && hasNoArgs;
	}

	public static boolean isGetter(Method method) {
		return method.getName().length() > 3 && method.getName().startsWith("get")
				&& method.getReturnType() != void.class;
	}

	public static SortedMap<String, JmxStats<?>> fetchNameToJmxStats(Object objectWithJmxStats) {
		SortedMap<String, JmxStats<?>> attributeToJmxStats = new TreeMap<>();
		SortedMap<String, Method> attributeToGetter = fetchNameToJmxStatsGetter(objectWithJmxStats);
		for (String attrName : attributeToGetter.keySet()) {
			Method getter = attributeToGetter.get(attrName);
			JmxStats<?> currentJmxStats;
			try {
				currentJmxStats = (JmxStats<?>) getter.invoke(objectWithJmxStats);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
			attributeToJmxStats.put(attrName, currentJmxStats);
		}
		return attributeToJmxStats;
	}

	public static SortedMap<String, Method> fetchNameToJmxStatsGetter(Object objectWithJmxStats) {
		SortedMap<String, Method> attributeToJmxStatsGetter = new TreeMap<>();
		Method[] methods = objectWithJmxStats.getClass().getMethods();
		for (Method method : methods) {
			if (isGetterOfJmxStats(method) && method.isAnnotationPresent(JmxAttribute.class)) {
				String currentJmxStatsName = extractFieldNameFromGetter(method);
				attributeToJmxStatsGetter.put(currentJmxStatsName, method);
			}
		}
		return attributeToJmxStatsGetter;
	}

	public static Map<String, Method> fetchNameToSimpleAttributeGetter(Object objectWithJmxStats) {
		Map<String, Method> attributeToJmxStatsGetter = new HashMap<>();
		Method[] methods = objectWithJmxStats.getClass().getMethods();
		for (Method method : methods) {
			if (isGetterOfSimpleType(method) && method.isAnnotationPresent(JmxAttribute.class)) {
				String currentAttrName = extractFieldNameFromGetter(method);
				attributeToJmxStatsGetter.put(currentAttrName, method);
			}
		}
		return attributeToJmxStatsGetter;
	}

	public static Map<String, Method> fetchNameToListAttributeGetter(Object objectWithJmxStats) {
		Map<String, Method> attributeToJmxStatsGetter = new HashMap<>();
		Method[] methods = objectWithJmxStats.getClass().getMethods();
		for (Method method : methods) {
			if (isGetterOfList(method) && method.isAnnotationPresent(JmxAttribute.class)) {
				String currentAttrName = extractFieldNameFromGetter(method);
				attributeToJmxStatsGetter.put(currentAttrName, method);
			}
		}
		return attributeToJmxStatsGetter;
	}

	public static Map<String, Method> fetchNameToArrayAttributeGetter(Object objectWithJmxStats) {
		Map<String, Method> attributeToJmxStatsGetter = new HashMap<>();
		Method[] methods = objectWithJmxStats.getClass().getMethods();
		for (Method method : methods) {
			if (isGetterOfArray(method) && method.isAnnotationPresent(JmxAttribute.class)) {
				String currentAttrName = extractFieldNameFromGetter(method);
				attributeToJmxStatsGetter.put(currentAttrName, method);
			}
		}
		return attributeToJmxStatsGetter;
	}

	public static Map<String, Method> fetchNameToThrowableAttributeGetter(Object objectWithJmxStats) {
		Map<String, Method> attributeToJmxStatsGetter = new HashMap<>();
		Method[] methods = objectWithJmxStats.getClass().getMethods();
		for (Method method : methods) {
			if (isGetterOfThrowable(method) && method.isAnnotationPresent(JmxAttribute.class)) {
				String currentAttrName = extractFieldNameFromGetter(method);
				attributeToJmxStatsGetter.put(currentAttrName, method);
			}
		}
		return attributeToJmxStatsGetter;
	}

	public static boolean isJmxMBean(Class<?> clazz) {
		return ConcurrentJmxMBean.class.isAssignableFrom(clazz);
	}

	public static <T> boolean isEmtpyOrNull(T[] array) {
		return array == null || array.length == 0;
	}

	public static <T> boolean hasSingleElement(T[] array) {
		return array != null && array.length == 1;
	}

	/**
	 * If value == null, returns empty string, otherwise return value.toString()
	 *
	 * @param value value
	 * @return returns empty string, if value == null, otherwise return value.toString()
	 */
	public static String stringOf(Object value) {
		if (value != null) {
			return value.toString();
		} else {
			return "";
		}
	}
}
