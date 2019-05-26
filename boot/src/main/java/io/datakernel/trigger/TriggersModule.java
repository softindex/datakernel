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

package io.datakernel.trigger;

import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import io.datakernel.jmx.KeyWithWorkerData;
import io.datakernel.service.BlockingService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.util.Initializable;
import io.datakernel.util.guice.GuiceUtils;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.OptionalInitializer;
import io.datakernel.util.guice.RequiredDependency;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPools;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.datakernel.util.guice.GuiceUtils.prettyPrintSimpleKeyName;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public final class TriggersModule extends AbstractModule implements Initializable<TriggersModule> {
	private Function<Key<?>, String> keyToString = GuiceUtils::prettyPrintSimpleKeyName;

	private final Map<Class<?>, Set<TriggerConfig<?>>> classSettings = new LinkedHashMap<>();
	private final Map<Key<?>, Set<TriggerConfig<?>>> keySettings = new LinkedHashMap<>();

	private static final class TriggerConfig<T> {
		private final Severity severity;
		private final String name;
		private final Function<T, TriggerResult> triggerFunction;

		TriggerConfig(Severity severity, String name,
				Function<T, TriggerResult> triggerFunction) {
			this.severity = severity;
			this.name = name;
			this.triggerFunction = triggerFunction;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TriggerConfig<?> that = (TriggerConfig<?>) o;
			return severity == that.severity &&
					Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(severity, name);
		}
	}

	private static final class TriggerRegistryRecord {
		private final Severity severity;
		private final String name;
		private final Supplier<TriggerResult> triggerFunction;

		private TriggerRegistryRecord(Severity severity, String name, Supplier<TriggerResult> triggerFunction) {
			this.severity = severity;
			this.name = name;
			this.triggerFunction = triggerFunction;
		}
	}

	public interface TriggersModuleService extends BlockingService {
	}

	private TriggersModule() {
	}

	public static TriggersModule create() {
		return new TriggersModule();
	}

	public TriggersModule withNaming(Function<Key<?>, String> keyToString) {
		this.keyToString = keyToString;
		return this;
	}

	public <T> TriggersModule with(Class<T> type, Severity severity, String name, Function<T, TriggerResult> triggerFunction) {
		Set<TriggerConfig<?>> triggerConfigs = classSettings.computeIfAbsent(type, $ -> new LinkedHashSet<>());

		if (!triggerConfigs.add(new TriggerConfig<>(severity, name, triggerFunction))) {
			throw new IllegalArgumentException("Cannot assign duplicate triggers");
		}

		return this;
	}

	public <T> TriggersModule with(Key<T> key, Severity severity, String name, Function<T, TriggerResult> triggerFunction) {
		Set<TriggerConfig<?>> triggerConfigs = keySettings.computeIfAbsent(key, $ -> new LinkedHashSet<>());

		if (!triggerConfigs.add(new TriggerConfig<>(severity, name, triggerFunction))) {
			throw new IllegalArgumentException("Cannot assign duplicate triggers");
		}

		return this;
	}

	@Override
	protected void configure() {
		bind(new Key<OptionalDependency<ServiceGraph>>() {}).require();
		bind(new Key<RequiredDependency<TriggersModuleService>>() {}).require();
	}

	@Provides
	TriggersModuleService service(Injector injector, Triggers triggers, OptionalInitializer<TriggersModule> optionalInitializer) {
		optionalInitializer.accept(this);
		return new TriggersModuleService() {
			@Override
			public void start() {
				initialize(injector);
			}

			@Override
			public void stop() {
			}
		};
	}

	@Provides
	Triggers getTriggersWatcher(Injector injector) {
		return Triggers.create();
	}

	@SuppressWarnings("unchecked")
	private void initialize(Injector injector) {
		Triggers triggers = injector.getInstance(Triggers.class);

		Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggersMap = new LinkedHashMap<>();

		// register singletons
		for (Key<?> k : injector.peekInstances().keySet()) {
			Key<Object> key = (Key<Object>) k;
			Object instance = injector.getInstance(key);
			KeyWithWorkerData internalKey = new KeyWithWorkerData(key);

			scanHasTriggers(triggersMap, internalKey, instance);
			scanClassSettings(triggersMap, internalKey, instance);
			scanKeySettings(triggersMap, internalKey, instance);
		}

		// register workers
		WorkerPools workerPools = injector.peekInstance(WorkerPools.class);
		if (workerPools != null) {
			for (WorkerPool workerPool : workerPools.getWorkerPools()) {
				for (Map.Entry<Key<?>, Object[]> entry : workerPool.peekInstances().entrySet()) {
					Key<?> key = entry.getKey();
					Object[] instances = entry.getValue();
					for (int i = 0; i < instances.length; i++) {
						Object instance = instances[i];
						KeyWithWorkerData k = new KeyWithWorkerData(key, workerPool, i);

						scanHasTriggers(triggersMap, k, instance);
						scanClassSettings(triggersMap, k, instance);
						scanKeySettings(triggersMap, k, instance);
					}
				}

			}
		}

		for (KeyWithWorkerData keyWithWorkerData : triggersMap.keySet()) {
			for (TriggerRegistryRecord registryRecord : triggersMap.getOrDefault(keyWithWorkerData, emptyList())) {
				triggers.addTrigger(registryRecord.severity, prettyPrintSimpleKeyName(keyWithWorkerData.getKey()), registryRecord.name, registryRecord.triggerFunction);
			}
		}
	}

	private void scanHasTriggers(Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggers, KeyWithWorkerData internalKey, Object instance) {
		if (instance instanceof HasTriggers) {
			((HasTriggers) instance).registerTriggers(new TriggerRegistry() {
				@Override
				public Key<?> getComponentKey() {
					return internalKey.getKey();
				}

				@Override
				public String getComponentName() {
					return keyToString.apply(internalKey.getKey());
				}

				@Override
				public void add(Severity severity, String name, Supplier<TriggerResult> triggerFunction) {
					triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>()).add(new TriggerRegistryRecord(severity, name, triggerFunction));
				}
			});
		}
	}

	@SuppressWarnings("unchecked")
	private void scanClassSettings(Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggers, KeyWithWorkerData internalKey, Object instance) {
		for (Class<?> clazz : classSettings.keySet()) {
			for (TriggerConfig<?> config : classSettings.get(clazz)) {
				if (clazz.isAssignableFrom(instance.getClass())) {
					triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>())
							.add(new TriggerRegistryRecord(config.severity, config.name, () ->
									((TriggerConfig<Object>) config).triggerFunction.apply(instance)));
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void scanKeySettings(Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggers, KeyWithWorkerData internalKey, Object instance) {
		Key<Object> key = (Key<Object>) internalKey.getKey();
		for (TriggerConfig<?> config : keySettings.getOrDefault(key, emptySet())) {
			triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>())
					.add(new TriggerRegistryRecord(config.severity, config.name, () ->
							((TriggerConfig<Object>) config).triggerFunction.apply(instance)));
		}
	}

}
