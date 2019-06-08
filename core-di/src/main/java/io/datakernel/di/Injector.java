package io.datakernel.di;

import io.datakernel.di.error.CannotConstructException;
import io.datakernel.di.error.CyclicDependensiesException;
import io.datakernel.di.error.NoBindingsInScopeException;
import io.datakernel.di.error.UnsatisfiedDependenciesException;
import io.datakernel.di.module.*;
import io.datakernel.di.util.BindingUtils;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

@SuppressWarnings("unused")
public class Injector {
	@Nullable
	private final Injector parent;

	private final Trie<Scope, Map<Key<?>, Binding<?>>> bindings;
	private final Map<Key<?>, Binding<?>> localBindings;
	private final Map<Key<?>, Object> instances;

	protected static final class SynchronizedInjector extends Injector {
		protected SynchronizedInjector(@Nullable Injector parent, Trie<Scope, Map<Key<?>, Binding<?>>> bindings, Map<Key<?>, Object> instances) {
			super(parent, bindings, instances);
		}

		@Override
		synchronized public <T> @NotNull T getInstance(@NotNull Key<T> key) {
			return super.getInstance(key);
		}

		@Override
		synchronized public <T> @Nullable T getInstanceOrNull(@NotNull Key<T> key) {
			return super.getInstanceOrNull(key);
		}

		@Override
		synchronized public <T> @NotNull T createInstance(@NotNull Key<T> key) {
			return super.createInstance(key);
		}

		@Override
		synchronized public <T> @Nullable T createInstanceOrNull(@NotNull Key<T> key) {
			return super.createInstanceOrNull(key);
		}

		@Override
		synchronized public <T> @Nullable T peekInstance(@NotNull Key<T> key) {
			return super.peekInstance(key);
		}

		@Override
		synchronized public Map<Key<?>, Object> peekInstances() {
			return super.peekInstances();
		}

		@Override
		synchronized public boolean hasInstance(@NotNull Key<?> type) {
			return super.hasInstance(type);
		}
	}

	private static final Object[] NO_OBJECTS = new Object[0];
	private static final Object NO_KEY = new Object();

	private Injector(@Nullable Injector parent, Trie<Scope, Map<Key<?>, Binding<?>>> bindings, Map<Key<?>, Object> instances) {
		this.parent = parent;
		this.bindings = bindings;
		this.localBindings = bindings.get();
		this.instances = instances;
	}

	public static Injector of(Module... modules) {
		Module module = Modules.combine(Modules.combine(modules), new DefaultModule());
		return compile(null, new HashMap<>(), true, module.getBindings(), module.getBindingTransformers(), module.getBindingGenerators());
	}

	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return compile(null, new HashMap<>(), true, bindings, emptyMap(), emptyMap());
	}

	public static Injector compile(@Nullable Injector parent, Map<Key<?>, Object> instances, boolean threadsafe,
			@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings,
			@NotNull Map<Integer, BindingTransformer<?>> bindingTransformers,
			@NotNull Map<Type, Set<BindingGenerator<?>>> bindingGenerators) {
		Injector injector = threadsafe ?
				new SynchronizedInjector(parent, bindings, instances) :
				new Injector(parent, bindings, instances);

		// well, can't do anything better than that
		bindings.get().put(Key.of(Injector.class), Binding.toInstance(injector));

		BindingUtils.completeBindings(bindings, bindingTransformers, bindingGenerators);

		Map<Key<?>, Set<Binding<?>>> unsatisfied = BindingUtils.getUnsatisfiedDependencies(bindings);
		if (!unsatisfied.isEmpty()) {
			throw new UnsatisfiedDependenciesException(injector, unsatisfied);
		}

		Set<Key<?>[]> cycles = BindingUtils.getCycles(bindings);
		if (!cycles.isEmpty()) {
			throw new CyclicDependensiesException(injector, cycles);
		}

		return injector;
	}

	@NotNull
	public <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T> T getInstance(@NotNull Key<T> key) {
		T instance = (T) instances.getOrDefault(key, NO_KEY);
		if (instance != NO_KEY) {
			return instance;
		}
		instance = doCreateInstanceOrNull(key);
		instances.put(key, instance);
		if (instance == null) {
			throw new CannotConstructException(this, key, localBindings.get(key));
		}
		return instance;
	}

	public <T> T getInstanceOr(@NotNull Class<T> type, T defaultValue) {
		T instanceOrNull = getInstanceOrNull(type);
		return instanceOrNull != null ? instanceOrNull : defaultValue;
	}

	public <T> T getInstanceOr(@NotNull Key<T> key, T defaultValue) {
		T instanceOrNull = getInstanceOrNull(key);
		return instanceOrNull != null ? instanceOrNull : defaultValue;
	}

	@Nullable
	public <T> T getInstanceOrNull(@NotNull Class<T> type) {
		return getInstanceOrNull(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> T getInstanceOrNull(@NotNull Key<T> key) {
		T instance = (T) instances.getOrDefault(key, NO_KEY);
		if (instance != NO_KEY) {
			return instance;
		}
		instance = doCreateInstanceOrNull(key);
		instances.put(key, instance);
		return instance;
	}

	@NotNull
	public <T> T createInstance(@NotNull Class<T> type) {
		return createInstance(Key.of(type));
	}

	@NotNull
	public <T> T createInstance(@NotNull Key<T> key) {
		T instance = doCreateInstanceOrNull(key);
		if (instance == null) {
			throw new CannotConstructException(this, key, localBindings.get(key));
		}
		return instance;
	}

	public <T> T createInstanceOr(@NotNull Class<T> type, T defaultValue) {
		T instance = createInstanceOrNull(type);
		return instance != null ? instance : defaultValue;
	}

	public <T> T createInstanceOr(@NotNull Key<T> key, T defaultValue) {
		T instance = createInstanceOrNull(key);
		return instance != null ? instance : defaultValue;
	}

	@Nullable
	public <T> T createInstanceOrNull(@NotNull Class<T> type) {
		return createInstanceOrNull(Key.of(type));
	}

	@Nullable
	public <T> T createInstanceOrNull(@NotNull Key<T> key) {
		return doCreateInstanceOrNull(key);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> T doCreateInstanceOrNull(@NotNull Key<T> key) {
		Binding<?> binding = localBindings.get(key);
		if (binding != null) {
			Dependency[] dependencies = binding.getDependencies();
			if (dependencies.length == 0) {
				return (T) binding.getFactory().create(NO_OBJECTS);
			}
			Object[] dependencyInstances = new Object[dependencies.length];
			for (int i = 0; i < dependencies.length; i++) {
				Dependency dependency = dependencies[i];
				Key<?> dependencyKey = dependency.getKey();
				Object dependencyInstance = instances.get(dependencyKey);
				if (dependencyInstance == null) {
					dependencyInstance = doCreateInstanceOrNull(dependencyKey);
					if (dependencyInstance != null) {
						instances.put(dependencyKey, dependencyInstance);
					}
				}
				if (dependencyInstance == null && dependency.isRequired()) {
					throw new CannotConstructException(this, dependencyKey, localBindings.get(dependencyKey));
				}
				dependencyInstances[i] = dependencyInstance;
			}
			return (T) binding.getFactory().create(dependencyInstances);
		}
		if (parent != null) {
			return parent.getInstanceOrNull(key);
		}
		return null;
	}

	@Nullable
	public <T> T peekInstance(@NotNull Class<T> type) {
		return peekInstance(Key.of(type));
	}

	@Nullable
	@SuppressWarnings("unchecked")
	public <T> T peekInstance(@NotNull Key<T> key) {
		return (T) instances.get(key);
	}

	public boolean hasInstance(@NotNull Class<?> type) {
		return hasInstance(Key.of(type));
	}

	public boolean hasInstance(@NotNull Key<?> type) {
		return instances.get(type) != null;
	}

	public Map<Key<?>, Object> peekInstances() {
		Map<Key<?>, Object> result = new HashMap<>();
		for (Map.Entry<Key<?>, Object> entry : instances.entrySet()) {
			Key<?> key = entry.getKey();
			Object value = entry.getValue();
			if (hasBinding(key) && value != null) {
				result.put(key, value);
			}
		}
		return result;
	}

	@Nullable
	public Injector getParent() {
		return parent;
	}

	public Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		return bindings;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Class<T> type) {
		return (Binding<T>) localBindings.get(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Key<T> key) {
		return (Binding<T>) localBindings.get(key);
	}

	public boolean hasBinding(Class<?> type) {
		return hasBinding(Key.of(type));
	}

	public boolean hasBinding(Key<?> key) {
		return localBindings.containsKey(key);
	}

	public Injector enterScope(@NotNull Scope scope) {
		return enterScope(scope, new HashMap<>(), isThreadSafe());
	}

	public Injector enterScope(@NotNull Scope scope, @NotNull Map<Key<?>, Object> instances, boolean threadsafe) {
		Trie<Scope, Map<Key<?>, Binding<?>>> subBindings = bindings.get(scope);
		if (subBindings == null) {
			throw new NoBindingsInScopeException(this, scope);
		}
		return threadsafe ?
				new SynchronizedInjector(this, subBindings, instances) :
				new Injector(this, subBindings, instances);
	}

	public boolean isThreadSafe() {
		return this.getClass() == SynchronizedInjector.class;
	}

}
