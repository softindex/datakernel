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

package io.datakernel.serializer.asm;

import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@SuppressWarnings("unchecked")
public final class Annotations {
	private Annotations() {
	}

	@Nullable
	public static <A extends Annotation> A findAnnotation(Class<A> type, Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType() == type)
				return (A) annotation;
		}
		return null;
	}

	public static boolean annotated(Class<? extends Annotation> type, Annotation[] annotations) {
		return findAnnotation(type, annotations) != null;
	}

	public static boolean annotated(String annotationName, Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotationName.equals(annotation.annotationType().getSimpleName()))
				return true;
		}
		return false;
	}

	public static <V, A extends Annotation> V annotationValue(Class<A> type, Annotation[] annotations, V defaultValue) {
		A a = findAnnotation(type, annotations);
		if (a == null)
			return defaultValue;
		try {
			Method value = type.getMethod("value");
			return (V) value.invoke(a);
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

}
