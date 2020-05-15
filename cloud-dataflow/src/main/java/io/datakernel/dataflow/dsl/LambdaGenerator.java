package io.datakernel.dataflow.dsl;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public final class LambdaGenerator {

	@SuppressWarnings("unchecked")
	public static Class<Object> getReturnType(Object functional) {
		Class<?> cls = functional.getClass();
		for (Method method : cls.getMethods()) {
			if (!Modifier.isStatic(method.getModifiers()) && !method.isSynthetic() && !method.isBridge() && !method.isDefault()) {
				return (Class<Object>) method.getReturnType();
			}
		}
		throw new IllegalArgumentException("Failed to get a return type for functional class " + cls.getSimpleName());
	}
}
