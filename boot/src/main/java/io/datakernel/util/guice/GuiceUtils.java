package io.datakernel.util.guice;

import com.google.inject.*;
import com.google.inject.spi.BindingScopingVisitor;
import com.google.inject.spi.DefaultBindingScopingVisitor;
import io.datakernel.util.SimpleType;
import io.datakernel.worker.WorkerPoolScope;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

public final class GuiceUtils {
	private GuiceUtils() {
	}

	public static boolean isSingleton(Binding<?> binding) {
		return binding.acceptScopingVisitor(new BindingScopingVisitor<Boolean>() {
			@Override
			public Boolean visitNoScoping() {
				return false;
			}

			@Override
			public Boolean visitScopeAnnotation(Class<? extends Annotation> visitedAnnotation) {
				return visitedAnnotation.equals(Singleton.class);
			}

			@Override
			public Boolean visitScope(Scope visitedScope) {
				return visitedScope.equals(Scopes.SINGLETON);
			}

			@Override
			public Boolean visitEagerSingleton() {
				return true;
			}
		});
	}

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
					String valueStr = (value instanceof String ? "\"" + value + "\"" : value.toString());
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
		Type type = key.getTypeLiteral().getType();
		return (key.getAnnotation() != null ? prettyPrintAnnotation(key.getAnnotation()) + " " : "") +
				SimpleType.ofType(type).getSimpleName();
	}

	public static String prettyPrintKeyName(Key<?> key) {
		Type type = key.getTypeLiteral().getType();
		return (key.getAnnotation() != null ? prettyPrintAnnotation(key.getAnnotation()) + " " : "") +
				SimpleType.ofType(type).getName();
	}

	public static Integer extractWorkerId(Binding<?> binding) {
		return binding.acceptScopingVisitor(new DefaultBindingScopingVisitor<Integer>() {
			@Override
			public Integer visitScope(Scope scope) {
				return scope instanceof WorkerPoolScope ? ((WorkerPoolScope) scope).getCurrentWorkerId() : null;
			}
		});
	}
}
