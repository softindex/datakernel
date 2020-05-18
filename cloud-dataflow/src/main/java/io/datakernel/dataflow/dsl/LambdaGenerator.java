package io.datakernel.dataflow.dsl;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.function.Function;
import java.util.function.Predicate;

public final class LambdaGenerator {

	public static <T, R> Function<T, R> generateMapper(AST.LambdaExpression expression) {
		throw new UnsupportedOperationException("todo: support actual lambdas with codegen");
	}

	public static <T> Predicate<T> generatePredicate(AST.LambdaExpression expression) {
		throw new UnsupportedOperationException("todo: support actual lambdas with codegen");
	}

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
