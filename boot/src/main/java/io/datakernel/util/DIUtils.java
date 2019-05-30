/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.di.Key;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

public final class DIUtils {
	public static String prettyPrintAnnotation(Annotation annotation) {
		StringBuilder sb = new StringBuilder();
		Method[] methods = annotation.annotationType().getDeclaredMethods();
		boolean first = true;
		if (methods.length != 0) {
			for (Method m : methods) {
				try {
					Object value = m.invoke(annotation);
					if (value.equals(m.getDefaultValue()))
						continue;
					String valueStr = value instanceof String ? "\"" + value + "\"" : value.toString();
					String methodName = m.getName();
					if ("value".equals(methodName) && first) {
						sb.append(valueStr);
						first = false;
					} else {
						sb.append(first ? "" : ",").append(methodName).append("=").append(valueStr);
						first = false;
					}
				} catch (ReflectiveOperationException ignored) {
				}
			}
		}
		String simpleName = annotation.annotationType().getSimpleName();
		return "@" + ("NamedImpl".equals(simpleName) ? "Named" : simpleName) + (first ? "" : "(" + sb + ")");
	}

	public static String prettyPrintSimpleKeyName(Key<?> key) {
		Type type = key.getType();
		return (key.getAnnotation() != null ?
				prettyPrintAnnotation(key.getAnnotation()) + " " :
				key.getAnnotationType() != null ?
						"@" + RecursiveType.of(key.getAnnotationType()).getSimpleName() + " " :
						"") + RecursiveType.of(type).getSimpleName();
	}

	public static String prettyPrintKeyName(Key<?> key) {
		Type type = key.getType();
		return (key.getAnnotation() != null ? prettyPrintAnnotation(key.getAnnotation()) + " " : "") +
				RecursiveType.of(type).getName();
	}

}
