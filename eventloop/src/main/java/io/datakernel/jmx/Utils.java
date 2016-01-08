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
import java.util.SortedMap;
import java.util.TreeMap;

import static io.datakernel.util.Preconditions.checkArgument;

public class Utils {

	public static String extractFieldNameFromGetter(Method method) {
		checkArgument(isGetter(method));

		String getterName = method.getName();
		String firstLetter = getterName.substring(3, 4);
		String restOfName = getterName.substring(4);
		return firstLetter.toLowerCase() + restOfName;
	}

	public static boolean isGetterOfJmxStats(Method method) {
		boolean returnsJmxStats = JmxStats.class.isAssignableFrom(method.getReturnType());
		return isGetter(method) && returnsJmxStats;
	}

	public static boolean isGetter(Method method) {
		return method.getName().length() > 3 && method.getName().startsWith("get");
	}

	public static SortedMap<String, JmxStats<?>> fetchNameToJmxStats(Object objectWithJmxStats) {
		SortedMap<String, JmxStats<?>> attributeToJmxStats = new TreeMap<>();
		Method[] methods = objectWithJmxStats.getClass().getMethods();
		for (Method method : methods) {
			if (isGetterOfJmxStats(method)) {
				String currentJmxStatsName = extractFieldNameFromGetter(method);
				JmxStats<?> currentJmxStats;
				try {
					currentJmxStats = (JmxStats<?>) method.invoke(objectWithJmxStats);
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
				attributeToJmxStats.put(currentJmxStatsName, currentJmxStats);
			}
		}
		return attributeToJmxStats;
	}
}
