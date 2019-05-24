package io.datakernel.di;

import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.flattenMultimap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.joining;

public final class Injector {
	@Nullable
	private final Scope scope;
	@Nullable
	private final Injector parent;

	private final Trie<Scope, Map<Key<?>, Binding<?>>> bindings;
	private final Map<Key<?>, Binding<?>> localBindings;
	private final Map<Key<?>, Object> instances = new HashMap<>();

	private Injector(@Nullable Scope scope, @Nullable Injector parent, Trie<Scope, Map<Key<?>, Binding<?>>> bindings, Map<Key<?>, Binding<?>> localBindings) {
		this.scope = scope;
		this.parent = parent;
		this.bindings = bindings;
		this.localBindings = localBindings;
	}

	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		Injector injector = new Injector(null, null, bindings, bindings.get());
		bindings.get().put(Key.of(Injector.class), Binding.constant(injector));

		ReflectionUtils.addImplicitBindings(bindings);

		Set<Key<?>[]> cycles = ReflectionUtils.getCycles(bindings);

		if (cycles.isEmpty()) {
			return injector;
		}

		String detail = cycles.stream()
				.map(cycle ->
						Stream.concat(Arrays.stream(cycle), Stream.of(cycle[0]))
								.map(Key::getDisplayString)
								.collect(joining(" -> ", "", " -> ...")))
				.collect(joining("\n"));
		throw new RuntimeException("cyclic dependencies detected:\n" + detail);
	}

	public static Injector ofScope(@NotNull Scope scope, @NotNull Injector outerScopeInjector, @NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return new Injector(scope, outerScopeInjector, bindings, bindings.get());
	}

	public static Injector create(Module... modules) {
		Module module = Modules.combine(modules);
		Function<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolver = module.getConflictResolvers()::get;
		return of(module.getBindings().map(localBindings -> flattenMultimap(localBindings, conflictResolver)));
	}

	@Nullable
	public Scope getScope() {
		return scope;
	}

	@Nullable
	public Injector getParent() {
		return parent;
	}

	public Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		return bindings;
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
		Binding<?> binding = localBindings.get(key);
		if (binding != null) {
			return binding.getFactory().create(generateDependencies(binding.getDependencies()));
		}
		if (parent != null) {
			return parent.generateInstance(key);
		}
		return null;
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

	public Map<Key<?>, Object> peekInstances() {
		return unmodifiableMap(instances);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Key<T> key) {
		return (Binding<T>) localBindings.get(key);
	}

	public Injector enterScope(Scope scope) {
		Trie<Scope, Map<Key<?>, Binding<?>>> sub = bindings.get(scope);
		if (sub == null) {
			throw new RuntimeException("tried to enter a scope " + scope + "that was not represented by any binding");
		}
		return new Injector(scope, this, sub, sub.get());
	}

	public Injector enterScope(Scope scope, Map<Key<?>, Binding<?>> extraBindings) {
		Trie<Scope, Map<Key<?>, Binding<?>>> sub = this.bindings.get(scope);
		if (sub == null) {
			throw new RuntimeException("tried to enter a scope " + scope + "that was not represented by any binding");
		}
		Map<Key<?>, Binding<?>> newLocalBindings = new HashMap<>(sub.get());
		newLocalBindings.putAll(extraBindings);
		return new Injector(scope, this, sub, newLocalBindings);
	}
}
