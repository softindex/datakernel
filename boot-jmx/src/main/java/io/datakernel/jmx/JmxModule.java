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

import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.Initializable;
import io.datakernel.common.MemSize;
import io.datakernel.common.StringFormatUtils;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.jmx.ValueStats;
import io.datakernel.jmx.JmxMBeans.JmxCustomTypeAdapter;
import io.datakernel.launcher.LauncherService;
import io.datakernel.trigger.Severity;
import io.datakernel.trigger.Triggers.TriggerWithResult;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPools;

import javax.management.DynamicMBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.datakernel.common.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Turns on support of Jmx in application.
 * <br>
 * Automatically builds MBeans for parts of application and adds Jmx attributes and operations to it.
 */
public final class JmxModule extends AbstractModule implements Initializable<JmxModule> {
	public static final Duration REFRESH_PERIOD_DEFAULT = Duration.ofSeconds(1);
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 50;

	private final Set<Object> globalSingletons = new HashSet<>();

	private final Map<Key<?>, MBeanSettings> keyToSettings = new HashMap<>();
	private final Map<Type, MBeanSettings> typeToSettings = new HashMap<>();
	private final Map<Key<?>, String> keyToObjectNames = new HashMap<>();
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes = new HashMap<>();
	private final Map<Type, Key<?>> globalMBeans = new HashMap<>();

	private Duration refreshPeriod = REFRESH_PERIOD_DEFAULT;
	private int maxJmxRefreshesPerOneCycle = MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT;
	private boolean withScopes = true;

	private JmxModule() {
	}

	public static JmxModule create() {
		return new JmxModule()
				.withCustomType(Duration.class, StringFormatUtils::formatDuration, StringFormatUtils::parseDuration)
				.withCustomType(Period.class, StringFormatUtils::formatPeriod, StringFormatUtils::parsePeriod)
				.withCustomType(Instant.class, StringFormatUtils::formatInstant, StringFormatUtils::parseInstant)
				.withCustomType(LocalDateTime.class, StringFormatUtils::formatLocalDateTime, StringFormatUtils::parseLocalDateTime)
				.withCustomType(MemSize.class, StringFormatUtils::formatMemSize, StringFormatUtils::parseMemSize)
				.withCustomType(TriggerWithResult.class, TriggerWithResult::toString)
				.withCustomType(Severity.class, Severity::toString)
				.withGlobalSingletons(ByteBufPool.getStats());
	}

	public JmxModule withRefreshPeriod(Duration refreshPeriod) {
		checkArgument(refreshPeriod.toMillis() > 0, "Duration of refresh period should be a positive value");
		this.refreshPeriod = refreshPeriod;
		return this;
	}

	public JmxModule withMaxJmxRefreshesPerOneCycle(int max) {
		checkArgument(max > 0, "Number of JMX refreshes should be a positive value");
		this.maxJmxRefreshesPerOneCycle = max;
		return this;
	}

	public <T> JmxModule withModifier(Key<?> key, String attrName, AttributeModifier<T> modifier) {
		keyToSettings.computeIfAbsent(key, $ -> MBeanSettings.defaultSettings())
				.withModifier(attrName, modifier);
		return this;
	}

	public <T> JmxModule withModifier(Type type, String attrName, AttributeModifier<T> modifier) {
		typeToSettings.computeIfAbsent(type, $ -> MBeanSettings.defaultSettings())
				.withModifier(attrName, modifier);
		return this;
	}

	public JmxModule withOptional(Key<?> key, String attrName) {
		keyToSettings.computeIfAbsent(key, $ -> MBeanSettings.defaultSettings())
				.withIncludedOptional(attrName);
		return this;
	}

	public JmxModule withOptional(Type type, String attrName) {
		typeToSettings.computeIfAbsent(type, $ -> MBeanSettings.defaultSettings())
				.withIncludedOptional(attrName);
		return this;
	}

	public JmxModule withHistogram(Key<?> key, String attrName, int[] histogramLevels) {
		return withOptional(key, attrName + "_histogram")
				.withModifier(key, attrName, (ValueStats attribute) ->
						attribute.setHistogramLevels(histogramLevels));
	}

	public JmxModule withHistogram(Class<?> clazz, String attrName, int[] histogramLevels) {
		return withHistogram(Key.of(clazz), attrName, histogramLevels);
	}

	public JmxModule withGlobalMBean(Type type, String named) {
		return withGlobalMBean(type, Key.ofType(type, named));
	}

	public JmxModule withGlobalMBean(Type type, Key<?> key) {
		globalMBeans.put(type, key);
		return this;
	}

	public JmxModule withObjectName(Key<?> key, String objectName) {
		this.keyToObjectNames.put(key, objectName);
		return this;
	}

	public JmxModule withScopes(boolean withScopes) {
		this.withScopes = withScopes;
		return this;
	}

//	public JmxModule withObjectName(Type type, String objectName) {
//		return withObjectName(Key.of(type), objectName);
//	}

	public <T> JmxModule withCustomType(Class<T> type, Function<T, String> to, Function<String, T> from) {
		this.customTypes.put(type, new JmxCustomTypeAdapter<>(to, from));
		return this;
	}

	public <T> JmxModule withCustomType(Class<T> type, Function<T, String> to) {
		this.customTypes.put(type, new JmxCustomTypeAdapter<>(to));
		return this;
	}

	public JmxModule withGlobalSingletons(Object... instances) {
		this.globalSingletons.addAll(asList(instances));
		return this;
	}

	@Export
	@ProvidesIntoSet
	LauncherService start(Injector injector, JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory) {
		return new LauncherService() {
			@Override
			public CompletableFuture<?> start() {
				doStart(injector, jmxRegistry, mbeanFactory);
				return completedFuture(null);
			}

			@Override
			public CompletableFuture<?> stop() {
				return completedFuture(null);
			}
		};
	}

	private void doStart(Injector injector, JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory) {
		Map<Type, List<Object>> globalMBeanObjects = new HashMap<>();

		// register global singletons
		for (Object globalSingleton : globalSingletons) {
			Key<?> globalKey = Key.of(globalSingleton.getClass());
			jmxRegistry.registerSingleton(globalKey, globalSingleton, MBeanSettings.defaultSettings().withCustomTypes(customTypes));
		}

		// register singletons
		for (Map.Entry<Key<?>, Object> entry : injector.peekInstances().entrySet()) {
			Key<?> key = entry.getKey();
			Object instance = entry.getValue();
			if (instance == null) continue;
			jmxRegistry.registerSingleton(key, instance, ensureSettingsFor(key));

			Type type = key.getType();
			if (globalMBeans.containsKey(type)) {
				globalMBeanObjects.computeIfAbsent(type, type1 -> new ArrayList<>()).add(instance);
			}
		}

		// register workers
		WorkerPools workerPools = injector.peekInstance(WorkerPools.class);
		if (workerPools != null) {
			// populating workerPoolKeys map
			injector.peekInstances().entrySet().stream()
					.filter(entry -> entry.getKey().getRawType().equals(WorkerPool.class))
					.forEach(entry -> jmxRegistry.addWorkerPoolKey((WorkerPool) entry.getValue(), entry.getKey()));

			for (WorkerPool workerPool : workerPools.getWorkerPools()) {
				for (Map.Entry<Key<?>, WorkerPool.Instances<?>> entry : workerPool.peekInstances().entrySet()) {
					Key<?> key = entry.getKey();
					WorkerPool.Instances<?> workerInstances = entry.getValue();
					jmxRegistry.registerWorkers(workerPool, key, workerInstances.getList(), ensureSettingsFor(key));

					Type type = key.getType();
					if (globalMBeans.containsKey(type)) {
						for (Object instance : workerInstances) {
							globalMBeanObjects.computeIfAbsent(type, $ -> new ArrayList<>()).add(instance);
						}
					}
				}
			}
		}

		for (Type type : globalMBeanObjects.keySet()) {
			List<Object> objects = globalMBeanObjects.get(type);
			Key<?> key = globalMBeans.get(type);
			DynamicMBean globalMBean =
					mbeanFactory.createFor(objects, ensureSettingsFor(key), false);
			jmxRegistry.registerSingleton(key, globalMBean, null);
		}
	}

	private MBeanSettings ensureSettingsFor(Key<?> key) {
		MBeanSettings settings = MBeanSettings.defaultSettings()
				.withCustomTypes(customTypes);
		if (keyToSettings.containsKey(key)) {
			settings.merge(keyToSettings.get(key));
		}
		if (typeToSettings.containsKey(key.getType())) {
			settings.merge(typeToSettings.get(key.getType()));
		}
		return settings;
	}

	@Provides
	DynamicMBeanFactory mbeanFactory() {
		return JmxMBeans.factory(refreshPeriod, maxJmxRefreshesPerOneCycle);
	}

	@Provides
	JmxRegistry jmxRegistry(DynamicMBeanFactory mbeanFactory) {
		return JmxRegistry.create(ManagementFactory.getPlatformMBeanServer(), mbeanFactory, keyToObjectNames, customTypes)
				.withScopes(withScopes);
	}

}
