package io.datakernel.jmx.stats;

import io.datakernel.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.common.reflection.ReflectionUtils.isSimpleType;

public final class StatsUtils {
	public static boolean isJmxStats(Class<?> cls) {
		return JmxStats.class.isAssignableFrom(cls);
	}

	public static boolean isJmxRefreshableStats(Class<?> cls) {
		return JmxRefreshableStats.class.isAssignableFrom(cls);
	}

	public static void resetStats(Object instance) {
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithReset) {
				((JmxStatsWithReset) item).resetStats();
				return true;
			}
			return false;
		});
	}

	public static void setSmoothingWindow(Object instance, Duration smoothingWindowSeconds) {
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithSmoothingWindow) {
				((JmxStatsWithSmoothingWindow) item).setSmoothingWindow(smoothingWindowSeconds);
				return true;
			}
			return false;
		});
	}

	@Nullable
	public static Duration getSmoothingWindow(Object instance) {
		Set<Duration> result = new HashSet<>();
		visitFields(instance, item -> {
			if (item instanceof JmxStatsWithSmoothingWindow) {
				Duration smoothingWindow = ((JmxStatsWithSmoothingWindow) item).getSmoothingWindow();
				result.add(smoothingWindow);
				return true;
			}
			return false;
		});
		if (result.size() == 1) {
			return first(result);
		}
		return null;
	}

	private static void visitFields(Object instance, Predicate<Object> action) {
		if (instance == null) {
			return;
		}
		for (Method method : instance.getClass().getMethods()) {
			if (method.getParameters().length != 0 || !Modifier.isPublic(method.getModifiers())) {
				continue;
			}
			Class<?> returnType = method.getReturnType();
			if (returnType == void.class || isSimpleType(returnType)) {
				continue;
			}
			if (!method.isAnnotationPresent(JmxAttribute.class)) {
				continue;
			}
			Object fieldValue;
			try {
				fieldValue = method.invoke(instance);
			} catch (IllegalAccessException | InvocationTargetException e) {
				continue;
			}
			if (fieldValue == null) {
				continue;
			}
			if (action.test(fieldValue)) {
				continue;
			}
			if (Map.class.isAssignableFrom(returnType)) {
				for (Object item : ((Map<?, ?>) fieldValue).values()) {
					visitFields(item, action);
				}
			} else if (Collection.class.isAssignableFrom(returnType)) {
				for (Object item : (Collection<?>) fieldValue) {
					visitFields(item, action);
				}
			} else {
				visitFields(fieldValue, action);
			}
		}
	}
}
