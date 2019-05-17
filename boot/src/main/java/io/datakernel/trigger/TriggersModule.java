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
import io.datakernel.util.TypeT;
import io.datakernel.util.guice.GuiceUtils;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.OptionalInitializer;
import io.datakernel.util.guice.RequiredDependency;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPools;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.datakernel.util.CollectionUtils.getLast;
import static io.datakernel.util.guice.GuiceUtils.prettyPrintSimpleKeyName;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public final class TriggersModule extends AbstractModule implements Initializable<TriggersModule> {
	private Function<Key<?>, String> keyToString = GuiceUtils::prettyPrintSimpleKeyName;

	private final Map<Key<?>, List<TriggerRegistryRecorder>> singletonRegistryRecords = new HashMap<>();
	private final Map<KeyWithWorkerData, List<TriggerRegistryRecorder>> workerRegistryRecords = new HashMap<>();

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

	private final class TriggerRegistryRecorder implements TriggerRegistry {
		private final Key<?> key;
		private final List<TriggerRegistryRecord> records = new ArrayList<>();

		private TriggerRegistryRecorder(Key<?> key) {
			this.key = key;
		}

		@Override
		public Key<?> getComponentKey() {
			return key;
		}

		@Override
		public String getComponentName() {
			return keyToString.apply(key);
		}

		@Override
		public void add(Severity severity, String name, Supplier<TriggerResult> triggerFunction) {
			records.add(new TriggerRegistryRecord(severity, name, triggerFunction));
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
		bind(new TypeT<OptionalDependency<ServiceGraph>>() {}).asSingleton();
		bind(new TypeT<RequiredDependency<TriggersModuleService>>() {}).asSingleton();

		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return isSingleton(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (TriggersModule.this) {
					Key<T> key = provision.getBinding().getKey();
					currentlyProvidingSingletonKeys.add(key);
					if (provision.provision() != null) {
						singletonKeys.add(key);
					}
					currentlyProvidingSingletonKeys.remove(key);
				}
			}
		});
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return WorkerPoolModule.isWorkerScope(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (TriggersModule.this) {
					Key<T> key = provision.getBinding().getKey();
					Integer workerId = extractWorkerId(provision.getBinding());
					WorkerPool workerPool = extractWorkerPool(provision.getBinding());
					assert workerId != null && workerPool != null : provision.getBinding();

					currentlyProvidingWorkerKeys.put(key, new KeyWithWorkerData(key, workerPool, workerId));
					if (provision.provision() != null) {
						workerKeys.add(provision.getBinding().getKey());
					}
					currentlyProvidingWorkerKeys.remove(key);
				}
			}
		});
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return binding.getKey().equals(Key.get(TriggerRegistry.class));
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (TriggersModule.this) {
					TriggerRegistryRecorder triggerRegistry = (TriggerRegistryRecorder) provision.provision();
					if (triggerRegistry == null) {
						return;
					}
					@SuppressWarnings("deprecation")
					List<com.google.inject.spi.DependencyAndSource> dependencyChain = provision.getDependencyChain();
					for (int i = dependencyChain.size() - 1; i >= 0; i--) {
						Key<?> key = dependencyChain.get(i).getDependency().getKey();
						if (currentlyProvidingSingletonKeys.contains(key)) {
							singletonRegistryRecords.computeIfAbsent(key, $ -> new ArrayList<>()).add(triggerRegistry);
							break;
						}
						KeyWithWorkerData kwwd = currentlyProvidingWorkerKeys.get(key);
						if (kwwd != null) {
							workerRegistryRecords.computeIfAbsent(kwwd, $ -> new ArrayList<>()).add(triggerRegistry);
							break;
						}
					}
				}
			}
		});
	}

	@Provides
	TriggersModuleService service(Injector injector, Triggers triggers,
			OptionalInitializer<TriggersModule> optionalInitializer) {
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

	@Provides
	TriggerRegistry getTriggersRegistry() {
		return new TriggerRegistryRecorder(getLast(currentlyProvidingSingletonKeys));
	}

	@SuppressWarnings("unchecked")
	private void initialize(Injector injector) {
		Triggers triggers = injector.getInstance(Triggers.class);

		Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggersMap = new LinkedHashMap<>();

		// register singletons
		for (Key<?> k : singletonKeys) {
			Key<Object> key = (Key<Object>) k;
			Object instance = injector.getInstance(key);
			KeyWithWorkerData internalKey = new KeyWithWorkerData(key);

			scanHasTriggers(triggersMap, internalKey, instance);
			scanClassSettings(triggersMap, internalKey, instance);
			scanRegistryRecords(triggersMap, internalKey, singletonRegistryRecords.getOrDefault(key, emptyList()));
			scanKeySettings(triggersMap, internalKey, instance);
		}

		// register workers
		if (!workerKeys.isEmpty()) {
			WorkerPools workerPools = injector.getInstance(WorkerPools.class);
			for (Key<?> key : workerKeys) {
				for (WorkerPool workerPool : workerPools.getWorkerPools()) {
					List<?> instances = workerPool.getInstances(key);
					for (int i = 0; i < instances.size(); i++) {
						Object instance = instances.get(i);
						KeyWithWorkerData k = new KeyWithWorkerData(key, workerPool, i);

						scanHasTriggers(triggersMap, k, instance);
						scanClassSettings(triggersMap, k, instance);
						scanRegistryRecords(triggersMap, k, workerRegistryRecords.getOrDefault(k, emptyList()));
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

	private void scanRegistryRecords(Map<KeyWithWorkerData, List<TriggerRegistryRecord>> triggers, KeyWithWorkerData internalKey, List<TriggerRegistryRecorder> registryRecorders) {
		for (TriggerRegistryRecorder recorder : registryRecorders) {
			for (TriggerRegistryRecord record : recorder.records) {
				triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>()).add(record);
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
