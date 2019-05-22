package io.datakernel.di;

import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.util.RecursiveType;
import io.datakernel.util.ref.Ref;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;

import static io.datakernel.di.util.Utils.flattenMultimap;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class Injector {
	@Nullable
	private final Scope scope;
	@Nullable
	private final Injector parentInjector;
	private final Map<Key<?>, Binding<?>> bindings;
	private final Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings;

	private final HashMap<Key<?>, Object> instances = new HashMap<>();

	private Injector(@Nullable Scope scope, @Nullable Injector parentInjector,
					 Map<Key<?>, Binding<?>> bindings, Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings) {
		this.scope = scope;
		this.parentInjector = parentInjector;
		this.bindings = bindings;
		this.scopeBindings = scopeBindings;

		Binding<Injector> injectorBinding = Binding.of(Key.of(Injector.class), new Dependency[0], $ -> this);
		bindings.put(injectorBinding.getKey(), injectorBinding);
	}

	public static Injector of(Map<Key<?>, Binding<?>> bindings, Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings) {
		return new Injector(null, null, bindings, scopeBindings);
	}

	public static Injector ofScope(@NotNull Scope scope, @NotNull Injector outerScopeInjector,
								   Map<Key<?>, Binding<?>> bindings, Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings) {
		return new Injector(scope, outerScopeInjector, bindings, scopeBindings);
	}

	public static Injector create(Module... modules) {
		Module module = Modules.combine(modules);

		Function<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolver = module.getConflictResolvers()::get;

		Map<Key<?>, Binding<?>> bindings = flattenMultimap(module.getBindings(), conflictResolver);

		Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings = module.getScopeBindings().entrySet().stream()
				.collect(toMap(Entry::getKey, scopeEntry -> flattenMultimap(scopeEntry.getValue(), conflictResolver)));

		ReflectionUtils.addImplicitBindings(bindings);
		return of(bindings, scopeBindings);
	}

	@Nullable
	public Scope getScope() {
		return scope;
	}

	@NotNull
	public <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public synchronized <T> T getInstance(@NotNull Key<T> key) {
		Object instance = instances.get(key);
		if (instance != null) {
			return (T) instance;
		}
		T constructed = generateInstance(key);
		if (constructed == null) {
			throw new RuntimeException("cannot construct " + key);
		}
		return constructed;
	}

	public <T> Optional<T> getOptionalInstance(@NotNull Class<T> type) {
		return getOptionalInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	public synchronized <T> Optional<T> getOptionalInstance(@NotNull Key<T> key) {
		Object instance = instances.get(key);
		if (instance != null) {
			return Optional.of((T) instance);
		}
		return Optional.ofNullable(generateInstance(key));
	}

	public void inject(Object instance) {
		ReflectionUtils.inject(instance, this);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> T generateInstance(@NotNull Key<T> key) {
		if (key.getTypeT().getRawType() == Provider.class) {
			return generateProvider(key);
		}
		Binding<T> binding = (Binding<T>) bindings.get(key);
		if (binding == null) {
			return parentInjector != null ? parentInjector.generateInstance(key) : null;
		}
		T instance = binding.getFactory().create(generateDependencies(binding, false));
		instances.put(key, instance);
		binding.getFactory().lateinit(instance, generateDependencies(binding, true));
		return instance;
	}

	@SuppressWarnings("unchecked")
	private <T> T generateProvider(Key<T> key) {
		RecursiveType[] typeParams = RecursiveType.of(key.getTypeT()).getTypeParams();
		checkArgument(typeParams.length == 1);
		Binding<T> binding = (Binding<T>) bindings.get(new Key(typeParams[0].getTypeT(), key.getName()));
		if (binding == null) {
			return null;
		}
		Object[] args = generateDependencies(binding, false);
		Ref<Object[]> lateinitArgs = new Ref<>(null);
		Binding.Factory<T> factory = binding.getFactory();
		T instance = (T) new Provider<T>() {
			@Override
			public T provideNew() {
				T instance = factory.create(args);
				factory.lateinit(instance, lateinitArgs.get());
				return instance;
			}

			@Override
			public synchronized T provideSingleton() {
				return (T) instances.computeIfAbsent(key, $ -> provideNew());
			}
		};
		instances.put(key, instance);
		lateinitArgs.set(generateDependencies(binding, true));
		return instance;
	}

	private Object[] generateDependencies(Binding<?> binding, boolean lateinit) {
		return Arrays.stream(binding.getDependencies())
				.filter(dependency -> lateinit ^ !dependency.isPostponed())
				.map(dependency -> dependency.isRequired() ? getInstance(dependency.getKey()) : getOptionalInstance(dependency.getKey()))
				.toArray(Object[]::new);
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

	public boolean canProvide(@NotNull Class<?> type) {
		return canProvide(Key.of(type));
	}

	public boolean canProvide(@NotNull Key<?> type) {
		return instances.containsKey(type) || (parentInjector != null && parentInjector.canProvide(type));
	}

	public Map<Key<?>, Object> peekInstances() {
		return unmodifiableMap(instances);
	}

	public Map<Key<?>, Binding<?>> getBindings() {
		return unmodifiableMap(bindings);
	}

	public Map<Key<?>, Set<Dependency>> getDependencies() {
		return bindings.entrySet().stream()
				.collect(toMap(Entry::getKey, entry -> new HashSet<>(Arrays.asList(entry.getValue().getDependencies()))));
	}

	public boolean hasBinding(@NotNull Class<?> type) {
		return hasBinding(Key.of(type));
	}

	public boolean hasBinding(@NotNull Key<?> type) {
		return bindings.containsKey(type);
	}

	public <T> Binding<T> getBinding(@NotNull Class<T> type) {
		return getBinding(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	public <T> Binding<T> getBinding(@NotNull Key<T> type) {
		return (Binding<T>) bindings.get(type);
	}

	public Map<Key<?>, Binding<?>> getScopeBindings(Scope scope) {
		return unmodifiableMap(scopeBindings.get(scope));
	}

	public Map<Key<?>, Set<ScopedDependency>> getScopeDependencies(Scope scope) {
		Map<Key<?>, Binding<?>> map = scopeBindings.get(scope);
		return map.entrySet().stream()
				.collect(toMap(Entry::getKey,
						entry -> Arrays.stream(entry.getValue().getDependencies())
								.map(dependencyKey ->
										map.containsKey(dependencyKey.getKey()) ?
												ScopedDependency.ofScoped(scope, dependencyKey) :
												ScopedDependency.ofUnscoped(dependencyKey)
								)
								.collect(toSet())
						)
				);
	}

	public Map<Scope, Map<Key<?>, Binding<?>>> getScopeBindings() {
		return unmodifiableMap(scopeBindings);
	}

	public boolean hasScope(Scope scope) {
		return scopeBindings.containsKey(scope);
	}

	public Injector enterScope(Scope scope) {
		return new Injector(scope, this, scopeBindings.get(scope), scopeBindings);
	}

	public Injector enterScope(Scope scope, Map<Key<?>, Binding<?>> extraBindings) {
		Map<Key<?>, Binding<?>> bindings = new HashMap<>(scopeBindings.get(scope));
		bindings.putAll(extraBindings);
		return new Injector(scope, this, bindings, scopeBindings);
	}
}
