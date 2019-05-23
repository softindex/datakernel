package io.datakernel.di;

import io.datakernel.di.Binding.Factory;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.ReflectionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.checkArgument;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.*;

public final class Injector {
	@Nullable
	private final Scope scope;
	@Nullable
	private final Injector parentInjector;

	private final ScopedBindings bindings;
	private final Map<Key<?>, Binding<?>> localBindings;

	private final HashMap<Key<?>, Object> instances = new HashMap<>();

	private Injector(@Nullable Scope scope, @Nullable Injector parentInjector, ScopedBindings bindings, Map<Key<?>, Binding<?>> localBindings) {
		this.scope = scope;
		this.parentInjector = parentInjector;
		this.bindings = bindings;
		this.localBindings = localBindings;

//		Binding<Injector> injectorBinding = Binding.of(new Dependency[0], $ -> this);
//		bindings.put(Key.of(Injector.class), injectorBinding);
	}

	public static Injector of(Map<Key<?>, Binding<?>> bindings, Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings) {
		ReflectionUtils.addImplicitBindings(bindings);

		Set<Key<?>[]> cycles = ReflectionUtils.checkBindingGraph(bindings);

		if (!cycles.isEmpty()) {
			String detail = cycles.stream()
					.map(cycle ->
							Stream.concat(Arrays.stream(cycle), Stream.of(cycle[0]))
									.map(Key::getDisplayString)
									.collect(joining(" -> ", "", " -> ...")))
					.collect(Collectors.joining("\n"));
			throw new RuntimeException("cyclic dependencies detected:\n" + detail);
		}

		return new Injector(null, null, bindings, scopeBindings);
	}

	public static Injector ofScope(@NotNull Scope scope, @NotNull Injector outerScopeInjector,
								   Map<Key<?>, Binding<?>> bindings, Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings) {
		return new Injector(scope, outerScopeInjector, bindings, scopeBindings);
	}

	public static Injector create(Module... modules) {
		Module module = Modules.combine(modules);

//		Function<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolver = module.getConflictResolvers()::get;
//
//		Map<Key<?>, Binding<?>> bindings = flattenMultimap(module.getBindings(), conflictResolver);
//
//		Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings = module.getScopeBindings().entrySet().stream()
//				.collect(toMap(Entry::getKey, scopeEntry -> flattenMultimap(scopeEntry.getValue(), conflictResolver)));

//		return of(bindings, scopeBindings);
		return null;
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
		T instance = (T) instances.computeIfAbsent(key, this::generateInstance);
		if (instance == null) {
			throw new RuntimeException("cannot construct " + key);
		}
		return instance;
	}

	public <T> Optional<T> getOptionalInstance(@NotNull Class<T> type) {
		return getOptionalInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	public synchronized <T> Optional<T> getOptionalInstance(@NotNull Key<T> key) {
		return Optional.ofNullable((T) instances.computeIfAbsent(key, this::generateInstance));
	}

	@SuppressWarnings("unchecked")
	public void inject(Object instance) {
		BindingInitializer<Object> initializer = ReflectionUtils.injectingInitializer(Key.of((Class<Object>) instance.getClass()));
		initializer.getInitializer().apply(instance, generateDependencies(initializer.getDependencies()));
	}

	@Nullable
	private Object generateInstance(@NotNull Key<?> key) {
		if (key.getRawType() == Provider.class) {
			return generateProvider(key);
		}
		Binding<?> binding = localBindings.get(key);
		if (binding != null) {
			return binding.getFactory().create(generateDependencies(binding.getDependencies()));
		}
		if (parentInjector != null) {
			return parentInjector.generateInstance(key);
		}
		return null;
	}

	@Nullable
	private Object generateProvider(Key<?> key) {
		Type[] typeParams = key.getTypeParams();
		checkArgument(typeParams.length == 1);

		Binding<?> binding = localBindings.get(Key.ofType(typeParams[0], key.getName()));
		if (binding == null) {
			return null;
		}
		Object[] args = generateDependencies(binding.getDependencies());
		Factory<?> factory = binding.getFactory();
		return new Provider<Object>() {
			@Override
			public Object provideNew() {
				return factory.create(args);
			}

			@Override
			public synchronized Object provideSingleton() {
				return instances.computeIfAbsent(key, $ -> provideNew());
			}
		};
	}

	private Object[] generateDependencies(Dependency[] dependencies) {
		return Arrays.stream(dependencies)
				.map(dependency -> dependency.isRequired() ?
						getInstance(dependency.getKey()) :
						getOptionalInstance(dependency.getKey()).orElse(null))
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
