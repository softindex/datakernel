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

package io.datakernel.trigger;

import com.google.inject.*;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.spi.ProvisionListener;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.jmx.JmxRegistry;
import io.datakernel.service.BlockingService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import io.datakernel.util.guice.GuiceUtils;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.RequiredDependency;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.Iterators.getLast;
import static io.datakernel.trigger.Severity.*;
import static io.datakernel.util.guice.GuiceUtils.*;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public final class TriggersModule extends AbstractModule implements Initializable<TriggersModule> {
	private Function<Key<?>, String> keyToString = GuiceUtils::prettyPrintSimpleKeyName;

	private final Set<Key<?>> singletonKeys = new HashSet<>();
	private final Set<Key<?>> workerKeys = new HashSet<>();

	private final LinkedHashSet<Key<?>> currentlyProvidingSingletonKeys = new LinkedHashSet<>();
	private final LinkedHashMap<Key<?>, Integer> currentlyProvidingWorkerKeys = new LinkedHashMap<>();

	private final Map<Key<?>, List<TriggerRegistryRecorder>> singletonRegistryRecords = new HashMap<>();
	private final Map<KeyWithWorkerId, List<TriggerRegistryRecorder>> workerRegistryRecords = new HashMap<>();

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

	private static final class KeyWithWorkerId {
		private final Key<?> key;
		private final int workerId;

		private KeyWithWorkerId(Key<?> key) {
			this(key, -1);
		}

		private KeyWithWorkerId(Key<?> key, int workerId) {
			this.key = key;
			this.workerId = workerId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			KeyWithWorkerId that = (KeyWithWorkerId) o;
			return workerId == that.workerId &&
					Objects.equals(key, that.key);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, workerId);
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

	private interface TriggersInitializer extends BlockingService {
	}

	private TriggersModule() {
	}

	public static TriggersModule create() {
		return new TriggersModule();
	}

	public static TriggersModule defaultInstance() {
		return create()
				.with(Eventloop.class, HIGH, "fatalErrors", eventloop ->
						TriggerResult.ofError(eventloop.getStats().getFatalErrors()))
				.with(Eventloop.class, HIGH, "businessLogic", eventloop ->
						TriggerResult.ofValue(eventloop.getStats().getBusinessLogicTime().getSmoothedAverage(),
								businessLogicTime -> businessLogicTime > 100))
				.with(ThrottlingController.class, INFORMATION, "throttling", throttlingController ->
						TriggerResult.ofValue(throttlingController.getAvgThrottling(),
								throttling -> throttling > 0.01))
				.with(ThrottlingController.class, WARNING, "throttling", throttlingController ->
						TriggerResult.ofValue(throttlingController.getAvgThrottling(),
								throttling -> throttling > 0.1))
				.with(ThrottlingController.class, AVERAGE, "throttling", throttlingController ->
						TriggerResult.ofValue(throttlingController.getAvgThrottling(),
								throttling -> throttling > 0.3))
				.with(EventloopTaskScheduler.class, HIGH, "error", scheduler ->
						TriggerResult.ofError(scheduler.getStats().getExceptions()))
				.with(EventloopTaskScheduler.class, WARNING, "error", scheduler ->
						TriggerResult.ofValue(scheduler.getStats().getExceptions().getTotal(), count -> count != 0))
				.with(EventloopTaskScheduler.class, WARNING, "delay", scheduler ->
						TriggerResult.ofTimestamp(scheduler.getStats().getLastStartTimestamp(),
								scheduler.getPeriod() != null && scheduler.getStats().getCurrentDuration() > scheduler.getPeriod() * 3))
				.with(EventloopTaskScheduler.class, AVERAGE, "delay", scheduler ->
						TriggerResult.ofTimestamp(scheduler.getStats().getLastStartTimestamp(),
								scheduler.getPeriod() != null && scheduler.getStats().getCurrentDuration() > scheduler.getPeriod() * 10))
				;
	}

	public TriggersModule withNaming(Function<Key<?>, String> keyToString) {
		this.keyToString = keyToString;
		return this;
	}

	public <T> TriggersModule with(Class<T> type, Severity severity, String name,
	                               Function<T, TriggerResult> triggerFunction) {
		classSettings.computeIfAbsent(type, $ -> new LinkedHashSet<>()).add(new TriggerConfig<>(severity, name, triggerFunction));
		return this;
	}

	public <T> TriggersModule with(Key<T> key, Severity severity, String name,
	                               Function<T, TriggerResult> triggerFunction) {
		keySettings.computeIfAbsent(key, $ -> new LinkedHashSet<>()).add(new TriggerConfig<>(severity, name, triggerFunction));
		return this;
	}

	@Override
	protected void configure() {
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
					if (workerId == null) throw new AssertionError(provision.getBinding());
					currentlyProvidingWorkerKeys.put(key, workerId);
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
					if (triggerRegistry != null) {
						for (int i = provision.getDependencyChain().size() - 1; i >= 0; i--) {
							Key<?> key = provision.getDependencyChain().get(i).getDependency().getKey();
							if (currentlyProvidingSingletonKeys.contains(key)) {
								singletonRegistryRecords.computeIfAbsent(key, $ -> new ArrayList<>()).add(triggerRegistry);
								break;
							}
							if (currentlyProvidingWorkerKeys.keySet().contains(key)) {
								int workerId = currentlyProvidingWorkerKeys.get(key);
								workerRegistryRecords.computeIfAbsent(new KeyWithWorkerId(key, workerId), $ -> new ArrayList<>()).add(triggerRegistry);
								break;
							}
						}
					}
				}
			}
		});
		bind(new TypeLiteral<RequiredDependency<ServiceGraph>>() {}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<JmxRegistry>>() {}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<Triggers>>() {}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<TriggersInitializer>>() {}).asEagerSingleton();
	}

	@Provides
	@Singleton
	TriggersInitializer triggersInitializer(Injector injector, Triggers triggers,
	                                        OptionalDependency<Initializer<TriggersModule>> maybeInitializer) {
		maybeInitializer.ifPresent(initializer -> initializer.accept(this));
		return new TriggersInitializer() {
			@Override
			public void start() throws Exception {
				initialize(injector);
			}

			@Override
			public void stop() throws Exception {
			}
		};
	}

	@Provides
	@Singleton
	Triggers getTriggersWatcher(Injector injector) {
		return Triggers.create();
	}

	@Provides
	TriggerRegistry getTriggersRegistry() {
		return new TriggerRegistryRecorder(getLast(currentlyProvidingSingletonKeys.iterator()));
	}

	@SuppressWarnings("unchecked")
	private void initialize(Injector injector) {
		Triggers triggers = injector.getInstance(Triggers.class);

		Map<KeyWithWorkerId, List<TriggerRegistryRecord>> triggersMap = new LinkedHashMap<>();

		// register singletons
		for (Key<?> k : singletonKeys) {
			Key<Object> key = (Key<Object>) k;
			Object instance = injector.getInstance(key);
			Type type = key.getTypeLiteral().getType();
			KeyWithWorkerId internalKey = new KeyWithWorkerId(key);

			scanHasTriggers(triggersMap, internalKey, instance);
			scanClassSettings(triggersMap, internalKey, instance);
			scanRegistryRecords(triggersMap, internalKey, singletonRegistryRecords.getOrDefault(key, emptyList()));
			scanKeySettings(triggersMap, internalKey, instance);
		}

		// register workers
		if (!workerKeys.isEmpty()) {
			WorkerPools workerPools = injector.getInstance(WorkerPools.class);
			for (Key<?> k : workerKeys) {
				Key<Object> key = (Key<Object>) k;
				List<Object> instances = workerPools.getWorkerPoolObjects(key).getObjects();

				for (int i = 0; i < instances.size(); i++) {
					Object instance = instances.get(i);
					KeyWithWorkerId internalKey = new KeyWithWorkerId(key, i);

					scanHasTriggers(triggersMap, internalKey, instance);
					scanClassSettings(triggersMap, internalKey, instance);
					scanRegistryRecords(triggersMap, internalKey, workerRegistryRecords.getOrDefault(internalKey, emptyList()));
					scanKeySettings(triggersMap, internalKey, instance);
				}
			}
		}

		for (KeyWithWorkerId keyWithWorkerId : triggersMap.keySet()) {
			for (TriggerRegistryRecord registryRecord : triggersMap.getOrDefault(keyWithWorkerId, emptyList())) {
				triggers.addTrigger(registryRecord.severity, prettyPrintSimpleKeyName(keyWithWorkerId.key), registryRecord.name, registryRecord.triggerFunction);
			}
		}
	}

	private void scanHasTriggers(Map<KeyWithWorkerId, List<TriggerRegistryRecord>> triggers, KeyWithWorkerId internalKey, Object instance) {
		if (instance instanceof HasTriggers) {
			((HasTriggers) instance).registerTriggers(new TriggerRegistry() {
				@Override
				public Key<?> getComponentKey() {
					return internalKey.key;
				}

				@Override
				public String getComponentName() {
					return keyToString.apply(internalKey.key);
				}

				@Override
				public void add(Severity severity, String name, Supplier<TriggerResult> triggerFunction) {
					triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>()).add(new TriggerRegistryRecord(severity, name, triggerFunction));
				}
			});
		}
	}

	@SuppressWarnings("unchecked")
	private void scanClassSettings(Map<KeyWithWorkerId, List<TriggerRegistryRecord>> triggers, KeyWithWorkerId internalKey, Object instance) {
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

	private void scanRegistryRecords(Map<KeyWithWorkerId, List<TriggerRegistryRecord>> triggers, KeyWithWorkerId internalKey, List<TriggerRegistryRecorder> registryRecorders) {
		for (TriggerRegistryRecorder recorder : registryRecorders) {
			for (TriggerRegistryRecord record : recorder.records) {
				triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>()).add(record);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void scanKeySettings(Map<KeyWithWorkerId, List<TriggerRegistryRecord>> triggers, KeyWithWorkerId internalKey, Object instance) {
		Key<Object> key = (Key<Object>) internalKey.key;
		for (TriggerConfig<?> config : keySettings.getOrDefault(key, emptySet())) {
			triggers.computeIfAbsent(internalKey, $ -> new ArrayList<>())
					.add(new TriggerRegistryRecord(config.severity, config.name, () ->
							((TriggerConfig<Object>) config).triggerFunction.apply(instance)));
		}
	}

}
