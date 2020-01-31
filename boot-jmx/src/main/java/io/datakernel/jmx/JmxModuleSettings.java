package io.datakernel.jmx;

import io.datakernel.di.core.Key;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.function.Function;

@SuppressWarnings("unused")
public interface JmxModuleSettings {
	JmxModuleSettings withRefreshPeriod(Duration refreshPeriod);

	JmxModuleSettings withMaxJmxRefreshesPerOneCycle(int max);

	<T> JmxModuleSettings withModifier(Key<?> key, String attrName, AttributeModifier<T> modifier);

	<T> JmxModuleSettings withModifier(Type type, String attrName, AttributeModifier<T> modifier);

	JmxModuleSettings withOptional(Key<?> key, String attrName);

	JmxModuleSettings withOptional(Type type, String attrName);

	JmxModuleSettings withHistogram(Key<?> key, String attrName, int[] histogramLevels);

	JmxModuleSettings withHistogram(Class<?> clazz, String attrName, int[] histogramLevels);

	JmxModuleSettings withGlobalMBean(Type type, String named);

	JmxModuleSettings withGlobalMBean(Type type, Key<?> key);

	JmxModuleSettings withObjectName(Key<?> key, String objectName);

	JmxModuleSettings withScopes(boolean withScopes);

	<T> JmxModuleSettings withCustomType(Class<T> type, Function<T, String> to, Function<String, T> from);

	<T> JmxModuleSettings withCustomType(Class<T> type, Function<T, String> to);

	JmxModuleSettings withGlobalSingletons(Object... instances);
}
