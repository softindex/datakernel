package io.datakernel.di;

import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.util.RecursiveType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
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

	private final ConcurrentHashMap<Key<?>, Object> instances = new ConcurrentHashMap<>();

	private Injector(@Nullable Scope scope, @Nullable Injector parentInjector,
			Map<Key<?>, Binding<?>> bindings, Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings) {
		this.scope = scope;
		this.parentInjector = parentInjector;
		this.bindings = bindings;
		this.scopeBindings = scopeBindings;
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
		Function<Key<?>, BinaryOperator<Binding<?>>> conflictResolvers = flattenMultimap(module.getConflictResolvers(), (a, b) -> a)::get;
		Map<Key<?>, Binding<?>> bindings = flattenMultimap(module.getBindings(), conflictResolvers);
		Map<Scope, Map<Key<?>, Binding<?>>> scopeBindings = module.getScopeBindings().entrySet().stream()
				.collect(toMap(Entry::getKey, scopeEntry -> flattenMultimap(scopeEntry.getValue(), conflictResolvers)));
		return of(bindings, scopeBindings);
	}

	@Nullable
	public Scope getScope() {
		return scope;
	}

	public <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	public <T> T getInstance(@NotNull Key<T> key) {
		return (T) instances.computeIfAbsent(key, this::constructInstance);
	}

	@SuppressWarnings("unchecked")
	private <T> T constructInstance(@NotNull Key<T> key) {
		Binding<T> binding = (Binding<T>) bindings.get(key);
		if (binding != null) {
			Object[] args = new Object[binding.getDependencies().length];
			for (int i = 0; i < args.length; i++) {
				args[i] = getInstance(binding.getDependencies()[i]);
			}
			return binding.getConstructor().construct(args);
		}

		return constructSpecialInstance(key);
	}

	@SuppressWarnings("unchecked")
	private <T> T constructSpecialInstance(@NotNull Key<T> key) {
		if (key.equals(Key.of(Injector.class))) return (T) this;
		if (key.getTypeT().getType() == Provider.class) {
			RecursiveType[] typeParams = RecursiveType.of(key.getTypeT()).getTypeParams();
			checkArgument(typeParams.length == 1);
			Binding<T> binding = (Binding<T>) bindings.get(new Key(typeParams[0].getTypeT(), key.getName()));
			if (binding != null) {
				Object[] args = new Object[binding.getDependencies().length];
				for (int i = 0; i < args.length; i++) {
					args[i] = getInstance(binding.getDependencies()[i]);
				}
				Binding.Constructor<T> constructor = binding.getConstructor();
				return (T) new Provider<T>() {
					@Override
					public T provideNew() {
						return constructor.construct(args);
					}

					@Override
					public T provideSingleton() {
						return (T) instances.computeIfAbsent(key, $ -> provideNew());
					}
				};
			}
		}

		if (parentInjector != null) return parentInjector.constructInstance(key);

		throw new IllegalArgumentException();
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
		return unmodifiableMap(instances);
	}

	public Map<Key<?>, Binding<?>> getBindings() {
		return unmodifiableMap(bindings);
	}

	public Map<Key<?>, Set<Key<?>>> getDependencies() {
		return bindings.entrySet().stream()
				.collect(toMap(Entry::getKey, entry -> new HashSet<>(Arrays.asList(entry.getValue().getDependencies()))));
	}

	public boolean hasBinding(@NotNull Class<?> type) {
		return hasBinding(Key.of(type));
	}

	public boolean hasBinding(@NotNull Key<?> type) {
		return bindings.containsKey(type);
	}

	public Map<Key<?>, Binding<?>> getScopeBindings(Scope scope) {
		return unmodifiableMap(scopeBindings.get(scope));
	}

	public Map<Key<?>, Set<ScopedKey<?>>> getScopeDependencies(Scope scope) {
		Map<Key<?>, Binding<?>> map = scopeBindings.get(scope);
		return map.entrySet().stream()
				.collect(toMap(Entry::getKey,
						entry -> Arrays.stream(entry.getValue().getDependencies())
								.map(dependencyKey ->
										map.containsKey(dependencyKey) ?
												ScopedKey.ofScoped(scope, dependencyKey) :
												ScopedKey.ofUnscoped(dependencyKey)
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
