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
import java.util.function.*;

import static io.datakernel.util.guice.GuiceUtils.prettyPrintAnnotation;

public final class TriggersModule extends AbstractModule implements Initializer<TriggersModule> {
	private final Set<Key<?>> singletonKeys = new HashSet<>();
	private final Set<Key<?>> workerKeys = new HashSet<>();

	private Function<Key<?>, String> keyToString = TriggersModule::name;
	private BiFunction<Key<?>, Integer, String> workerKeyToString = TriggersModule::name;

	private static final class MonitoringConfig<T> {
		private final BiPredicate<Key<T>, T> matcher;
		private final Severity severity;
		private final String name;
		private final Function<T, TriggerResult> triggerFunction;

		MonitoringConfig(BiPredicate<Key<T>, T> matcher,
		                 Severity severity, String name,
		                 Function<T, TriggerResult> triggerFunction) {
			this.matcher = matcher;
			this.severity = severity;
			this.name = name;
			this.triggerFunction = triggerFunction;
		}
	}

	private final List<MonitoringConfig<?>> classSettings = new ArrayList<>();

	private TriggersModule() {
	}

	public static TriggersModule create() {
		return new TriggersModule();
	}

	public TriggersModule withNaming(Function<Key<?>, String> keyToString,
	                                 BiFunction<Key<?>, Integer, String> workerKeyToString) {
		this.keyToString = keyToString;
		this.workerKeyToString = workerKeyToString;
		return this;
	}

	public <T> TriggersModule with(Class<T> type, Severity severity, String name,
	                               Function<T, TriggerResult> triggerFunction) {
		return with((k, instance) -> type.isAssignableFrom(instance.getClass()), severity, name, triggerFunction);
	}

	public <T> TriggersModule with(Key<T> key, Severity severity, String name,
	                               Function<T, TriggerResult> triggerFunction) {
		return with((k, instance) -> k.equals(key), severity, name, triggerFunction);
	}

	public <T> TriggersModule with(BiPredicate<Key<T>, T> matcher, Severity severity, String name,
	                               Function<T, TriggerResult> triggerFunction) {
		classSettings.add(new MonitoringConfig<>(matcher, severity, name, triggerFunction));
		return this;
	}

	@Override
	public TriggersModule initialize(Consumer<TriggersModule> initializer) {
		return null;
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
			scanTriggers(triggers, key, instance, () -> keyToString.apply(key));
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
					scanTriggers(triggers, key, instance, () -> workerKeyToString.apply(key, finalI));
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T> void scanTriggers(Triggers triggers, Key<T> key, T instance, Supplier<String> naming) {
		String component = null;

		if (instance instanceof HasTriggers) {
			component = naming.get();
			String finalComponent = component;
			((HasTriggers) instance).registerTriggers((severity, name, triggerFunction) -> {
				triggers.addTrigger(severity, finalComponent, name, triggerFunction);
			});
		}

		for (Object entry : classSettings) {
			MonitoringConfig<T> config = (MonitoringConfig<T>) entry;
			if (config.matcher.test(key, instance)) {
				if (component == null) component = naming.get();
				triggers.addTrigger(config.severity, component, config.name, () -> config.triggerFunction.apply(instance));
			}
		}
	}

	private static String name(Key<?> key) {
		Type type = key.getTypeLiteral().getType();
		return (key.getAnnotation() != null ? prettyPrintAnnotation(key.getAnnotation()) + " " : "") +
				SimpleType.ofType(type).getSimpleName();
	}

	private static String name(Key<?> key, int workerIndex) {
		Type type = key.getTypeLiteral().getType();
		return (key.getAnnotation() != null ? prettyPrintAnnotation(key.getAnnotation()) + " " : "") +
				SimpleType.ofType(type).getSimpleName() +
				"[" + workerIndex + "]";
	}

}
