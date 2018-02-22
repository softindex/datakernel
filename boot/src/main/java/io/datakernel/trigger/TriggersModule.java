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
import io.datakernel.util.Initializer;
import io.datakernel.util.SimpleType;
import io.datakernel.util.guice.GuiceUtils;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class TriggersModule extends AbstractModule implements Initializer<TriggersModule> {
	public static final double REFRESH_PERIOD_DEFAULT = 1.0;
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 50;

	private final Set<Key<?>> singletonKeys = new HashSet<>();
	private final Set<Key<?>> workerKeys = new HashSet<>();

	private static final class MonitoringConfig<T> {
		private final BiPredicate<Key<T>, T> matcher;
		private final Severity severity;
		private final Function<T, String> triggerFunction;

		MonitoringConfig(BiPredicate<Key<T>, T> matcher,
		                 Severity severity,
		                 Function<T, String> triggerFunction) {
			this.matcher = matcher;
			this.severity = severity;
			this.triggerFunction = triggerFunction;
		}
	}

	private final List<MonitoringConfig<?>> classSettings = new ArrayList<>();

	private TriggersModule() {
	}

	public static TriggersModule create() {
		return new TriggersModule();
	}

	public <T> TriggersModule with(Class<T> type, Severity severity,
	                               Predicate<T> predicate, String description) {
		return with(type, severity, instance -> predicate.test(instance) ? description : null);
	}

	public <T> TriggersModule with(Class<T> type, Severity severity,
	                               Function<T, String> triggerFunction) {
		return with((k, instance) -> type.isAssignableFrom(instance.getClass()), severity, triggerFunction);
	}

	public <T> TriggersModule with(Key<T> key, Severity severity,
	                               Function<T, String> triggerFunction) {
		return with((k, instance) -> k.equals(key), severity, triggerFunction);
	}

	public <T> TriggersModule with(BiPredicate<Key<T>, T> matcher, Severity severity,
	                               Function<T, String> triggerFunction) {
		classSettings.add(new MonitoringConfig<>(matcher, severity, triggerFunction));
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
				synchronized (TriggersModule.this) {
					if (provision.provision() != null) {
						workerKeys.add(provision.getBinding().getKey());
					}
				}
			}
		});
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return GuiceUtils.isSingleton(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (TriggersModule.this) {
					if (provision.provision() != null) {
						singletonKeys.add(provision.getBinding().getKey());
					}
				}
			}
		});
	}

	@Provides
	@Singleton
	Triggers getTriggersWatcher(Injector injector, OptionalDependency<Set<Trigger>> providedTriggers) {
		return Triggers.create()
				.withInitializer(triggers -> initialize(triggers, injector, providedTriggers));
	}

	@SuppressWarnings("unchecked")
	private void initialize(Triggers triggers, Injector injector, OptionalDependency<Set<Trigger>> providedTriggers) {
		// directly provided triggers
		if (providedTriggers.isPresent()) {
			for (Trigger trigger : providedTriggers.get()) {
				triggers.addTrigger(trigger);
			}
		}

		// register singletons
		for (Key<?> k : singletonKeys) {
			Key<Object> key = (Key<Object>) k;
			Object instance = injector.getInstance(key);
			Type type = key.getTypeLiteral().getType();
			scanTriggers(triggers, key, instance, () -> name(key));
		}

		// register workers
		if (!workerKeys.isEmpty()) {
			WorkerPools workerPools = injector.getInstance(WorkerPools.class);
			for (Key<?> k : workerKeys) {
				Key<Object> key = (Key<Object>) k;
				List<Object> instances = workerPools.getWorkerPoolObjects(key).getObjects();

				for (int i = 0; i < instances.size(); i++) {
					Object instance = instances.get(i);
					final int finalI = i;
					scanTriggers(triggers, key, instance, () -> name(key, finalI));
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T> void scanTriggers(Triggers monitoring, Key<T> key, T instance, Supplier<String> naming) {
		String name = null;
		for (Object entry : classSettings) {
			MonitoringConfig<T> config = (MonitoringConfig<T>) entry;
			if (config.matcher.test(key, instance)) {
				if (name == null) name = naming.get();
				monitoring.addTrigger(
						config.severity, name, instance,
						config.triggerFunction);
			}
		}
	}

	private static String name(Key<?> key) {
		Type type = key.getTypeLiteral().getType();
		return SimpleType.ofType(type).getSimpleName();
	}

	private static String name(Key<?> key, int workerIndex) {
		Type type = key.getTypeLiteral().getType();
		return SimpleType.ofType(type).getSimpleName() + "[" + workerIndex + "]";
	}

}
