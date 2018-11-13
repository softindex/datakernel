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

import com.google.inject.*;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.name.Names;
import com.google.inject.spi.ProvisionListener;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.jmx.JmxMBeans.JmxCustomTypeAdapter;
import io.datakernel.service.BlockingService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.trigger.Severity;
import io.datakernel.trigger.Triggers.TriggerWithResult;
import io.datakernel.util.Initializable;
import io.datakernel.util.MemSize;
import io.datakernel.util.StringFormatUtils;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.OptionalInitializer;
import io.datakernel.util.guice.RequiredDependency;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;

import javax.management.DynamicMBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.guice.GuiceUtils.isSingleton;

/**
 * Turns on support of Jmx in application.
 * <br>
 * Automatically builds MBeans for parts of application and adds Jmx attributes and operations to it.
 */
public final class JmxModule extends AbstractModule implements Initializable<JmxModule> {
	public static final Duration REFRESH_PERIOD_DEFAULT = Duration.ofSeconds(1);
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 50;

	private final Set<Object> globalSingletons = new HashSet<>();
	private final Set<Key<?>> singletonKeys = new HashSet<>();
	private final Set<Key<?>> workerKeys = new HashSet<>();

	private final Map<Key<?>, MBeanSettings> keyToSettings = new HashMap<>();
	private final Map<Type, MBeanSettings> typeToSettings = new HashMap<>();
	private final Map<Key<?>, String> keyToObjectNames = new HashMap<>();
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes = new HashMap<>();

	private Duration refreshPeriod = REFRESH_PERIOD_DEFAULT;
	private int maxJmxRefreshesPerOneCycle = MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT;
	private final Map<Type, Key<?>> globalMBeans = new HashMap<>();

	public interface JmxModuleService extends BlockingService {
	}

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
		checkArgument(refreshPeriod.toMillis() > 0);
		this.refreshPeriod = refreshPeriod;
		return this;
	}

	public JmxModule withMaxJmxRefreshesPerOneCycle(int max) {
		checkArgument(max > 0);
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
		return withHistogram(Key.get(clazz), attrName, histogramLevels);
	}

	public JmxModule withGlobalMBean(Type type, String named) {
		return withGlobalMBean(type, Key.get(type, Names.named(named)));
	}

	public JmxModule withGlobalMBean(Type type, Key<?> key) {
		globalMBeans.put(type, key);
		return this;
	}

	public JmxModule withObjectName(Key<?> key, String objectName) {
		this.keyToObjectNames.put(key, objectName);
		return this;
	}

	public JmxModule withObjectName(Type type, String objectName) {
		return withObjectName(Key.get(type), objectName);
	}

	public <T> JmxModule withCustomType(Class<T> type, Function<T, String> to, Function<String, T> from) {
		this.customTypes.put(type, new JmxCustomTypeAdapter<>(to, from));
		return this;
	}

	public <T> JmxModule withCustomType(Class<T> type, Function<T, String> to) {
		this.customTypes.put(type, new JmxCustomTypeAdapter<>(to));
		return this;
	}

	public JmxModule withGlobalSingletons(Object... instances) {
		this.globalSingletons.addAll(Arrays.asList(instances));
		return this;
	}

	@Override
	protected void configure() {
		bind(new TypeLiteral<OptionalDependency<ServiceGraph>>() {}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<JmxModuleService>>() {}).asEagerSingleton();

		bindKeyListeners(binder(), this, b -> singletonKeys.add(b.getKey()), b -> workerKeys.add(b.getKey()));
	}

	public static void bindKeyListeners(Binder binder, Object lock, Consumer<Binding<?>> singletonBindingConsumer, Consumer<Binding<?>> workerBindingConsumer) {
		binder.bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return WorkerPoolModule.isWorkerScope(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (lock) {
					if (provision.provision() != null) {
						workerBindingConsumer.accept(provision.getBinding());
					}
				}
			}
		});
		binder.bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return isSingleton(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (lock) {
					if (provision.provision() != null) {
						singletonBindingConsumer.accept(provision.getBinding());
					}
				}
			}
		});
	}

	@Provides
	@Singleton
	JmxModuleService service(Injector injector, JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory,
			OptionalInitializer<JmxModule> optionalInitializer) {
		optionalInitializer.accept(this);
		return new JmxModuleService() {
			private MBeanSettings ensureSettingsFor(Key<?> key) {
				MBeanSettings settings = MBeanSettings.defaultSettings()
						.withCustomTypes(customTypes);
				if (keyToSettings.containsKey(key)) {
					settings.merge(keyToSettings.get(key));
				}
				if (typeToSettings.containsKey(key.getTypeLiteral().getType())) {
					settings.merge(typeToSettings.get(key.getTypeLiteral().getType()));
				}
				return settings;
			}

			@Override
			public void start() {

				Map<Type, List<Object>> globalMBeanObjects = new HashMap<>();

				// register global singletons
				for (Object globalSingleton : globalSingletons) {
					Key<?> globalKey = Key.get(globalSingleton.getClass());
					jmxRegistry.registerSingleton(globalKey, globalSingleton, MBeanSettings.defaultSettings().withCustomTypes(customTypes));
				}

				// register singletons
				for (Key<?> key : singletonKeys) {
					Object instance = injector.getInstance(key);
					jmxRegistry.registerSingleton(key, instance, ensureSettingsFor(key));

					Type type = key.getTypeLiteral().getType();
					if (globalMBeans.containsKey(type)) {
						globalMBeanObjects.computeIfAbsent(type, type1 -> new ArrayList<>()).add(instance);
					}
				}

				// register workers
				if (!workerKeys.isEmpty()) {
					WorkerPools workerPools = injector.getInstance(WorkerPools.class);
					for (WorkerPool workerPool : workerPools.getWorkerPools()) {
						for (Key<?> key : workerKeys) {
							List<?> objects = workerPool.getExistingInstances(key);
							if (objects == null){
								continue;
							}
							jmxRegistry.registerWorkers(workerPool, key, objects, ensureSettingsFor(key));

							Type type = key.getTypeLiteral().getType();
							if (globalMBeans.containsKey(type)) {
								for (Object workerObject : objects) {
									globalMBeanObjects.computeIfAbsent(type, type1 -> new ArrayList<>()).add(workerObject);
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

			@Override
			public void stop() {
			}
		};
	}

	@Provides
	DynamicMBeanFactory mbeanFactory() {
		return JmxMBeans.factory(refreshPeriod, maxJmxRefreshesPerOneCycle);
	}

	@Provides
	@Singleton
	JmxRegistry jmxRegistry(DynamicMBeanFactory mbeanFactory) {
		return JmxRegistry.create(ManagementFactory.getPlatformMBeanServer(), mbeanFactory, keyToObjectNames, customTypes);
	}
}
