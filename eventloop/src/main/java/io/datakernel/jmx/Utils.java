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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static io.datakernel.util.Preconditions.checkArgument;

public final class Utils {

	private Utils() {}

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

	public static boolean isGetterOfSimpleType(Method method) {
		boolean returnsSimpleType =
				boolean.class.isAssignableFrom(method.getReturnType())
						|| int.class.isAssignableFrom(method.getReturnType())
						|| long.class.isAssignableFrom(method.getReturnType())
						|| double.class.isAssignableFrom(method.getReturnType())
						|| String.class.isAssignableFrom(method.getReturnType());
		boolean hasNoArgs = method.getParameterTypes().length == 0;
		// TODO(vmykhalko): maybe also condisder "is*" getter for boolean instead of "get*" getter ?
		return isGetter(method) && returnsSimpleType && hasNoArgs;
	}

	public static boolean isGetter(Method method) {
		return method.getName().length() > 3 && method.getName().startsWith("get");
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
