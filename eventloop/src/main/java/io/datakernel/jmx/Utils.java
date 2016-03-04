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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;

final class Utils {

	private Utils() {}

	public static boolean isJmxStats(Class<?> clazz) {
		return JmxStats.class.isAssignableFrom(clazz);
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
		boolean isGetGetter = method.getName().length() > 3 && method.getName().startsWith("get") && doesntReturnVoid;

		return isIsGetter || isGetGetter;
	}

	public static String extractFieldNameFromGetter(Method getter) {
		checkArgument(isGetter(getter));

		if (getter.getName().startsWith("get")) {
			String getterName = getter.getName();
			String firstLetter = getterName.substring(3, 4);
			String restOfName = getterName.substring(4);
			return firstLetter.toLowerCase() + restOfName;
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

	public static <T> boolean hasSingleElement(T[] array) {
		return array != null && array.length == 1;
	}

	public static <T> List<T> filterNulls(List<T> src) {
		List<T> out = new ArrayList<>();
		for (T item : src) {
			if (item != null) {
				out.add(item);
			}
		}
		return out;
	}
}
