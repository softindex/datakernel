package io.datakernel.di;

import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

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
		synchronized public <T> @NotNull T getInstance(@NotNull Class<T> type) {
			return super.getInstance(type);
		}

		@Override
		synchronized public <T> @NotNull T getInstance(@NotNull Key<T> key) {
			return super.getInstance(key);
		}

		@Override
		synchronized public <T> @Nullable T getInstanceOrNull(@NotNull Class<T> type) {
			return super.getInstanceOrNull(type);
		}

		@Override
		synchronized public <T> @Nullable T getInstanceOrNull(@NotNull Key<T> key) {
			return super.getInstanceOrNull(key);
		}
	}

	private static final Object[] NO_OBJECTS = new Object[0];

	private Injector(@Nullable Injector parent, Trie<Scope, Map<Key<?>, Binding<?>>> bindings,
			Map<Key<?>, Object> instances) {
		this.parent = parent;
		this.bindings = bindings;
		this.localBindings = bindings.get();
		this.instances = instances;
	}

	public static Injector of(Module... modules) {
		Module module = Modules.combine(modules);
		return construct(null, new HashMap<>(), true, module.getBindings());
	}

	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return construct(null, new HashMap<>(), true, bindings);
	}

	public static Injector construct(@Nullable Injector parent, Map<Key<?>, Object> instances, boolean threadsafe,
			@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		Injector injector = threadsafe ?
				new SynchronizedInjector(parent, bindings, instances) :
				new Injector(parent, bindings, instances);
		bindings.get().put(Key.of(Injector.class), Binding.constant(injector));

		ReflectionUtils.addImplicitBindings(bindings);

		return injector;

//		Set<Key<?>[]> cycles = ReflectionUtils.getCycles(bindings);
//
//		if (cycles.isEmpty()) {
//			return injector;
//		}
//
//		String detail = cycles.stream()
//				.map(cycle ->
//						Stream.concat(Arrays.stream(cycle), Stream.of(cycle[0]))
//								.map(Key::getDisplayString)
//								.collect(joining(" -> ", "", " -> ...")))
//				.collect(joining("\n"));
//		throw new RuntimeException("cyclic dependencies detected:\n" + detail);
	}

	@NotNull
	public <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T> T getInstance(@NotNull Key<T> key) {
		T instance = (T) instances.computeIfAbsent(key, this::provideInstance);
		if (instance == null) throw new RuntimeException("cannot construct " + key);
		return instance;
	}

	@Nullable
	public <T> T getInstanceOrNull(@NotNull Class<T> type) {
		return getInstanceOrNull(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> T getInstanceOrNull(@NotNull Key<T> key) {
		return (T) instances.computeIfAbsent(key, this::provideInstance);
	}

	@Nullable
	protected Object provideInstance(@NotNull Key<?> key) {
		Binding<?> binding = localBindings.get(key);
		if (binding != null) {
			return binding.getFactory().create(getDependencies(binding.getDependencies()));
		}
		if (parent != null) {
			return parent.getInstanceOrNull(key);
		}
		return null;
	}

	private Object[] getDependencies(Dependency[] dependencies) {
		if (dependencies.length == 0) return NO_OBJECTS;
		Object[] instances = new Object[dependencies.length];
		for (int i = 0; i < dependencies.length; i++) {
			Dependency dependency = dependencies[i];
			instances[i] = dependency.isRequired() ?
					getDependency(dependency.getKey()) :
					getDependencyOrNull(dependency.getKey());
		}
		return instances;
	}

	@NotNull
	private <T> T getDependency(@NotNull Key<T> key) {
		T instance = getDependencyOrNull(key);
		if (instance == null) throw new RuntimeException("cannot construct " + key);
		return instance;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> T getDependencyOrNull(@NotNull Key<T> key) {
		T instance = (T) instances.get(key);
		if (instance != null) return instance;
		instance = (T) provideInstance(key);
		instances.put(key, instance);
		return instance;
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
		return instances.containsKey(type);
	}

	public Map<Key<?>, Object> peekInstances() {
		return instances;
	}

	@Nullable
	public Injector getParent() {
		return parent;
	}

	public boolean isThreadSafe() {
		return this.getClass() == SynchronizedInjector.class;
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
		return hasInstance(Key.of(type));
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
			throw new RuntimeException("tried to enter a scope " + scope + "that was not represented by any binding");
		}
		return threadsafe ?
				new SynchronizedInjector(this, subBindings, instances) :
				new Injector(this, subBindings, instances);
	}
}
