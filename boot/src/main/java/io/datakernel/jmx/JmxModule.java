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
import io.datakernel.config.ConfigConverters;
import io.datakernel.jmx.JmxMBeans.Transformer;
import io.datakernel.service.BlockingService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.trigger.Severity;
import io.datakernel.trigger.Triggers.TriggerWithResult;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import io.datakernel.util.MemSize;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.RequiredDependency;
import io.datakernel.worker.WorkerPoolModule;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.guice.GuiceUtils.isSingleton;

public final class JmxModule extends AbstractModule implements Initializable<JmxModule> {
	public static final Duration REFRESH_PERIOD_DEFAULT = Duration.ofSeconds(1);
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 50;

	private final Set<Key<?>> singletonKeys = new HashSet<>();
	private final Set<Key<?>> workerKeys = new HashSet<>();

	private final Map<Key<?>, MBeanSettings> keyToSettings = new HashMap<>();
	private final Map<Type, MBeanSettings> typeToSettings = new HashMap<>();
	private final Map<Key<?>, String> keyToObjectNames = new HashMap<>();
	private final Map<Type, Transformer<?>> customTypes = new HashMap<>();

	private Duration refreshPeriod = REFRESH_PERIOD_DEFAULT;
	private int maxJmxRefreshesPerOneCycle = MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT;
	private final Map<Type, Key<?>> globalMBeans = new HashMap<>();

	private interface JmxRegistratorService extends BlockingService {
	}

	private JmxModule() {
		customTypes.put(Duration.class, new Transformer<>(ConfigConverters::durationToString, ConfigConverters::parseDuration));
		customTypes.put(MemSize.class, new Transformer<>(MemSize::toString, MemSize::valueOf));
		customTypes.put(Period.class, new Transformer<>(ConfigConverters::periodToString, ConfigConverters::parsePeriod));
		customTypes.put(Instant.class, new Transformer<>(ConfigConverters::instantToString, ConfigConverters::parseInstant));
		customTypes.put(LocalDateTime.class, new Transformer<>(ConfigConverters::localDateTimeToString, ConfigConverters::parseLocalDateTime));
		customTypes.put(TriggerWithResult.class, new Transformer<>(TriggerWithResult::toString));
		customTypes.put(Severity.class, new Transformer<>(Severity::toString));
	}

	public static JmxModule create() {
		return new JmxModule();
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
				.addModifier(attrName, modifier);
		return this;
	}

	public <T> JmxModule withModifier(Type type, String attrName, AttributeModifier<T> modifier) {
		typeToSettings.computeIfAbsent(type, $ -> MBeanSettings.defaultSettings())
				.addModifier(attrName, modifier);
		return this;
	}

	public JmxModule withOptional(Key<?> key, String attrName) {
		keyToSettings.computeIfAbsent(key, $ -> MBeanSettings.defaultSettings())
				.addIncludedOptional(attrName);
		return this;
	}

	public JmxModule withOptional(Type type, String attrName) {
		typeToSettings.computeIfAbsent(type, $ -> MBeanSettings.defaultSettings())
				.addIncludedOptional(attrName);
		return this;
	}

	public JmxModule withHistogram(Key<?> key, String attrName, int[] histogramLevels) {
		return this
				.withOptional(key, attrName + "_histogram")
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
		this.customTypes.put(type, new Transformer<>(to, from));
		return this;
	}

	@Override
	protected void configure() {
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return WorkerPoolModule.isWorkerScope(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (JmxModule.this) {
					if (provision.provision() != null) {
						workerKeys.add(provision.getBinding().getKey());
					}
				}
			}
		});
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return isSingleton(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (JmxModule.this) {
					if (provision.provision() != null) {
						singletonKeys.add(provision.getBinding().getKey());
					}
				}
			}
		});
		bind(new TypeLiteral<RequiredDependency<ServiceGraph>>() {
		}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<JmxRegistratorService>>() {
		}).asEagerSingleton();
	}

	@Provides
	@Singleton
	JmxRegistratorService jmxRegistratorService(JmxRegistrator jmxRegistrator,
												OptionalDependency<Initializer<JmxModule>> maybeInitializer) {
		maybeInitializer.ifPresent(initializer -> initializer.accept(this));
		return new JmxRegistratorService() {
			@Override
			public void start() {
				jmxRegistrator.registerJmxMBeans();
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
	JmxRegistrator jmxRegistrator(Injector injector, JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory) {
		return JmxRegistrator.create(injector, singletonKeys, workerKeys, jmxRegistry, mbeanFactory,
				keyToSettings, typeToSettings, globalMBeans, customTypes);
	}

	@Provides
	@Singleton
	JmxRegistry jmxRegistry(DynamicMBeanFactory mbeanFactory) {
		return JmxRegistry.create(ManagementFactory.getPlatformMBeanServer(), mbeanFactory, keyToObjectNames, customTypes);
	}
}
