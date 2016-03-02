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

import static io.datakernel.util.Preconditions.checkArgument;

final class Utils {

	private Utils() {}

	public static String extractFieldNameFromGetter(Method method) {
		checkArgument(isGetter(method));

		String getterName = method.getName();
		String firstLetter = getterName.substring(3, 4);
		String restOfName = getterName.substring(4);
		return firstLetter.toLowerCase() + restOfName;
	}

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
		return method.getName().length() > 3 && method.getName().startsWith("get")
				&& method.getReturnType() != void.class;
	}

	public static <T> boolean hasSingleElement(T[] array) {
		return array != null && array.length == 1;
	}
}
