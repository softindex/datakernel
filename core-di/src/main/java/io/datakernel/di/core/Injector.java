package io.datakernel.di.core;

import io.datakernel.di.annotation.EagerSingleton;
import io.datakernel.di.impl.CompiledBinding;
import io.datakernel.di.impl.CompiledBindingLocator;
import io.datakernel.di.impl.Preprocessor;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.DefaultModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Types;
import io.datakernel.di.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static io.datakernel.di.core.BindingGenerator.REFUSING;
import static io.datakernel.di.core.BindingGenerator.combinedGenerator;
import static io.datakernel.di.core.BindingTransformer.IDENTITY;
import static io.datakernel.di.core.BindingTransformer.combinedTransformer;
import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.impl.CompiledBinding.missingOptionalBinding;
import static io.datakernel.di.util.Utils.next;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Injector is the main working component of the DataKernel DI.
 * <p>
 * It stores a trie of binding graphs and a cache of already made singletons.
 * <p>
 * Each injector is associated with exactly zero or one instance per {@link Key}.
 * <p>
 * Injector uses binding graph at the root of the trie to recursively create and then store instances of objects
 * associated with some {@link Key keys}.
 * Branches of the trie are used to {@link #enterScope enter scopes}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public final class Injector {
	public static final Key<Set<InstanceInjector<?>>> INSTANCE_INJECTORS_KEY = new Key<Set<InstanceInjector<?>>>() {};

	private static final class DependencyGraph {
		private final Scope[] scope;
		private final Map<Key<?>, Binding<?>> bindings;
		private final Map<Key<?>, CompiledBinding<?>> compiledBindings;
		private final Map<Key<?>, Integer> compiledIndexes;

		private DependencyGraph(Scope[] scope, Map<Key<?>, Binding<?>> bindings, Map<Key<?>, CompiledBinding<?>> compiledBindings, Map<Key<?>, Integer> compiledIndexes) {
			this.scope = scope;
			this.bindings = bindings;
			this.compiledBindings = compiledBindings;
			this.compiledIndexes = compiledIndexes;
		}
	}

	private final Map<Key<?>, CompiledBinding<?>> compiledBindings;
	private final Map<Key<?>, Integer> compiledIndexes;
	private final AtomicReferenceArray[] scopedInstances;
	private final Trie<Scope, DependencyGraph> scopeTree;
	@Nullable
	private final Injector parent;

	@SuppressWarnings("unchecked")
	private Injector(@Nullable Injector parent, Trie<Scope, DependencyGraph> scopeTree) {
		this.compiledBindings = scopeTree.get().compiledBindings;
		this.compiledIndexes = scopeTree.get().compiledIndexes;
		this.scopedInstances = parent == null ? new AtomicReferenceArray[1] : Arrays.copyOf(parent.scopedInstances, parent.scopedInstances.length + 1);
		this.scopedInstances[this.scopedInstances.length - 1] = new AtomicReferenceArray(scopeTree.get().compiledIndexes.size());
		this.scopedInstances[this.scopedInstances.length - 1].lazySet(0, this);
		this.scopeTree = scopeTree;
		this.parent = parent;
	}

	/**
	 * This constructor combines given modules (along with a {@link DefaultModule})
	 * and then {@link #compile(Injector, Module) compiles} them.
	 */
	public static Injector of(Module... modules) {
		return compile(null, Modules.combine(Modules.combine(modules), new DefaultModule()));
	}

	public static Injector of(@Nullable Injector parent, Module... modules) {
		return compile(parent, Modules.combine(Modules.combine(modules), new DefaultModule()));
	}

	/**
	 * This constructor is a shortcut for threadsafe {@link #compile(Injector, Scope[], Trie, Map, BindingTransformer, BindingGenerator) compile}
	 * with no instance overrides and no multibinders, transformers or generators.
	 */
	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return compile(null, UNSCOPED, bindings.map(Utils::toMultimap), emptyMap(), IDENTITY, REFUSING);
	}

	/**
	 * This constructor threadsafely {@link #compile(Injector, Scope[], Trie, Map, BindingTransformer, BindingGenerator) compiles}
	 * given module, extracting bindings and their multibinders, transformers and generators from it, with no instance overrides
	 */
	public static Injector compile(@Nullable Injector parent, Module module) {
		return compile(parent, UNSCOPED, module.getBindings(),
				module.getMultibinders(),
				combinedTransformer(module.getBindingTransformers()),
				combinedGenerator(module.getBindingGenerators()));
	}

	/**
	 * The most full-fledged compile method that allows you to create an Injector of any configuration.
	 * <p>
	 * Note that any injector <b>always</b> sets a binding of Injector key to provide itself.
	 *
	 * @param parent           parent injector that is called when this injector cannot fulfill the request
	 * @param scope            the scope of the injector, can be described as 'prefix of the root' of the binding trie,
	 *                         used when {@link #enterScope entering scopes}
	 * @param bindingsMultimap a trie of binding set graph with multiple possible conflicting bindings per key
	 *                         that are resolved as part of the compilation.
	 * @param multibinders     multibindinders that are called on every binding conflict
	 * @param transformer      a transformer that is called on every binding once (see {@link BindingTransformer#combinedTransformer})
	 * @param generator        a generator that is called on every missing binding (see {@link BindingGenerator#combinedGenerator})
	 * @see #enterScope
	 */
	public static Injector compile(@Nullable Injector parent,
			Scope[] scope,
			@NotNull Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindingsMultimap,
			@NotNull Map<Key<?>, Multibinder<?>> multibinders,
			@NotNull BindingTransformer<?> transformer,
			@NotNull BindingGenerator<?> generator) {

		Trie<Scope, Map<Key<?>, Binding<?>>> bindings = Preprocessor.resolveConflicts(bindingsMultimap, multibinders);

		Preprocessor.completeBindingGraph(bindings, transformer, generator);

		Set<Key<?>> known = new HashSet<>(bindings.get().keySet());
		known.add(Key.of(Injector.class)); // injector is hardcoded in and will always be present
		if (parent != null) {
			known.addAll(parent.compiledBindings.keySet());
		}

		Map<Key<?>, Set<Map.Entry<Key<?>, Binding<?>>>> unsatisfied = Preprocessor.getUnsatisfiedDependencies(known, bindings);

		if (!unsatisfied.isEmpty()) {
			throw new DIException(unsatisfied.entrySet().stream()
					.map(entry -> {
						Key<?> required = entry.getKey();
						String displayString = required.getDisplayString();
						return entry.getValue().stream()
								.map(binding -> {
									Key<?> key = binding.getKey();
									String displayAndLocation = key.getDisplayString() + " " + Utils.getLocation(binding.getValue());
									Class<?> rawType = key.getRawType();
									if (Modifier.isStatic(rawType.getModifiers()) || !required.getRawType().equals(rawType.getEnclosingClass())) {
										return displayAndLocation;
									}
									String indent = new String(new char[Utils.getKeyDisplayCenter(key) + 2]).replace('\0', ' ');
									return displayAndLocation + "\n\t\t" + indent + "^- this is a non-static inner class with implicit dependency on its enclosing class";
								})
								.collect(joining("\n\t\t- ", "\tkey " + displayString + " required to make:\n\t\t- ", ""));
					})
					.collect(joining("\n", "Unsatisfied dependencies detected:\n", "\n")));
		}

		Set<Key<?>[]> cycles = Preprocessor.getCyclicDependencies(bindings);
		if (!cycles.isEmpty()) {
			throw new DIException(cycles.stream()
					.map(Utils::drawCycle)
					.collect(joining("\n\n", "Cyclic dependencies detected:\n\n", "\n")));
		}

		return new Injector(parent, compileBindingsTrie(parent != null ? parent.scopedInstances.length : 0, UNSCOPED, bindings, parent != null ? parent.compiledBindings : emptyMap()));
	}

	protected static Trie<Scope, DependencyGraph> compileBindingsTrie(int scope, Scope[] path,
			Trie<Scope, Map<Key<?>, Binding<?>>> bindingsTrie,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsOfParent) {
		DependencyGraph dependencyGraph = compileBindings(scope, path, bindingsTrie.get(), compiledBindingsOfParent);
		Map<Scope, Trie<Scope, DependencyGraph>> children = new HashMap<>();
		bindingsTrie.getChildren().forEach((childScope, trie) -> {
			Map<Key<?>, CompiledBinding<?>> compiledBindingsCopy = new HashMap<>(compiledBindingsOfParent);
			compiledBindingsCopy.putAll(dependencyGraph.compiledBindings);
			children.put(childScope,
					compileBindingsTrie(scope + 1, next(path, childScope), bindingsTrie.get(childScope), compiledBindingsCopy));
		});
		return new Trie<>(dependencyGraph, children);
	}

	protected static DependencyGraph compileBindings(int scope, Scope[] path,
			Map<Key<?>, Binding<?>> bindings,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsOfParent) {
		boolean threadsafe = path.length == 0 || path[path.length - 1].isThreadsafe();
		Map<Key<?>, CompiledBinding<?>> compiledBindings = new HashMap<>();
		Map<Key<?>, Integer> compiledIndexes = new HashMap<>();
		compiledBindings.put(Key.of(Injector.class),
				scope == 0 ?
						new CompiledBinding<Object>() {
							volatile Object instance;

							@Override
							public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object instance = this.instance;
								if (instance != null) return instance;
								this.instance = scopedInstances[scope].get(0);
								return this.instance;
							}

							@Override
							public Object createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								throw new UnsupportedOperationException();
							}
						} :
						new CompiledBinding<Object>() {
							@Override
							public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return scopedInstances[scope].get(0); // directly provided in Injector constructor
							}

							@Override
							public Object createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								throw new UnsupportedOperationException();
							}
						});
		compiledIndexes.put(Key.of(Injector.class), 0);
		for (Key<?> key : bindings.keySet()) {
			compiledBindings.put(key,
					compileBinding(scope, threadsafe, key, bindings, compiledBindingsOfParent, compiledBindings, compiledIndexes));
		}
		bindings.put(Key.of(Injector.class), Binding.to(() -> {throw new AssertionError();}));
		compiledBindingsOfParent.forEach(compiledBindings::putIfAbsent);
		return new DependencyGraph(path, bindings, compiledBindings, compiledIndexes);
	}

	private static CompiledBinding<?> compileBinding(int scope, boolean threadsafe, Key<?> key,
			Map<Key<?>, Binding<?>> bindings,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsOfParent,
			Map<Key<?>, CompiledBinding<?>> compiledBindings,
			Map<Key<?>, Integer> compiledIndexes) {
		if (compiledBindings.containsKey(key)) {
			return compiledBindings.get(key);
		}
		Binding<?> binding = bindings.get(key);
		if (binding == null) {
			return compiledBindingsOfParent.containsKey(key) ?
					compiledBindingsOfParent.get(key) :
					missingOptionalBinding();
		}
		int index = compiledIndexes.size();
		compiledIndexes.put(key, index);
		CompiledBinding<?> compiledBinding = binding.getCompiler().compile(
				new CompiledBindingLocator() {
					@Override
					public @NotNull <Q> CompiledBinding<Q> get(Key<Q> key) {
						//noinspection unchecked
						return (CompiledBinding<Q>) compileBinding(scope, threadsafe, key, bindings,
								compiledBindingsOfParent, compiledBindings, compiledIndexes);
					}
				}, threadsafe,
				scope, index);
		compiledBindings.put(key, compiledBinding);
		return compiledBinding;
	}

	/**
	 * @see #getInstance(Key)
	 */
	@NotNull
	public <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.ofType(type));
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T> T getInstance(@NotNull Key<T> key) {
		CompiledBinding<?> binding = compiledBindings.get(key);
		if (binding != null) {
			return (T) binding.getInstance(scopedInstances, -1);
		}
		throw DIException.cannotConstruct(key, null);
	}

	/**
	 * @see #getInstanceOrNull(Key)
	 */
	@Nullable
	public <T> T getInstanceOrNull(@NotNull Class<T> type) {
		return getInstanceOrNull(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> T getInstanceOrNull(@NotNull Key<T> key) {
		CompiledBinding<?> binding = compiledBindings.get(key);
		return binding != null ? (T) binding.getInstance(scopedInstances, -1) : null;
	}

	/**
	 * @see #getInstanceOr(Key, Object)
	 */
	public <T> T getInstanceOr(@NotNull Class<T> type, T defaultValue) {
		return getInstanceOr(Key.of(type), defaultValue);
	}

	/**
	 * Same as {@link #getInstanceOrNull(Key)}, but replaces <code>null</code> with given default value.
	 */
	public <T> T getInstanceOr(@NotNull Key<T> key, T defaultValue) {
		T instance = getInstanceOrNull(key);
		return instance != null ? instance : defaultValue;
	}

	@NotNull
	public <T> T createInstance(@NotNull Class<T> type) {
		return createInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T> T createInstance(@NotNull Key<T> key) {
		CompiledBinding<?> binding = compiledBindings.get(key);
		if (binding != null) {
			return (T) binding.createInstance(scopedInstances, -1);
		}
		throw DIException.cannotConstruct(key, null);
	}

	@Nullable
	public <T> T createInstanceOrNull(@NotNull Class<T> type) {
		return createInstanceOrNull(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> T createInstanceOrNull(@NotNull Key<T> key) {
		CompiledBinding<?> binding = compiledBindings.get(key);
		return binding != null ? (T) binding.createInstance(scopedInstances, -1) : null;
	}

	@Nullable
	public <T> T peekInstance(@NotNull Class<T> type) {
		return peekInstance(Key.of(type));
	}

	/**
	 * This method returns an instance only if it already was created by a {@link #getInstance} call before,
	 * it does not trigger instance creation.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	public <T> T peekInstance(@NotNull Key<T> key) {
		Integer index = compiledIndexes.get(key);
		if (index == null) return null;
		return (T) scopedInstances[scopedInstances.length - 1].get(index);
	}

	@NotNull
	public <T> InstanceProvider<T> getInstanceProvider(@NotNull Class<T> type) {
		return getInstanceProvider(Key.of(type));
	}

	@NotNull
	public <T> InstanceProvider<T> getInstanceProvider(@NotNull Key<T> key) {
		return getInstance(Key.ofType(Types.parameterized(InstanceProvider.class, key.getType()), key.getName()));
	}

	@NotNull
	public <T> InstanceFactory<T> getInstanceFactory(@NotNull Class<T> type) {
		return getInstanceFactory(Key.of(type));
	}

	@NotNull
	public <T> InstanceFactory<T> getInstanceFactory(@NotNull Key<T> key) {
		return getInstance(Key.ofType(Types.parameterized(InstanceFactory.class, key.getType()), key.getName()));
	}

	@NotNull
	public <T> InstanceInjector<T> getInstanceInjector(@NotNull Class<T> type) {
		return getInstanceInjector(Key.of(type));
	}

	@NotNull
	public <T> InstanceInjector<T> getInstanceInjector(@NotNull Key<T> key) {
		return getInstance(Key.ofType(Types.parameterized(InstanceInjector.class, key.getType()), key.getName()));
	}

	/**
	 * This method checks if an instance for this key was created by a {@link #getInstance} call before.
	 */
	public boolean hasInstance(@NotNull Class<?> type) {
		return hasInstance(Key.of(type));
	}

	/**
	 * This method checks if an instance for this key was created by a {@link #getInstance} call before.
	 */
	public boolean hasInstance(@NotNull Key<?> key) {
		Integer index = compiledIndexes.get(key);
		if (index == null) return false;
		return scopedInstances[scopedInstances.length - 1].get(index) != null;
	}

	/**
	 * This method returns a copy of the injector cache - a map of all already created instances.
	 */
	public Map<Key<?>, Object> peekInstances() {
		Map<Key<?>, Object> result = new HashMap<>();
		for (Map.Entry<Key<?>, Integer> entry : compiledIndexes.entrySet()) {
			Key<?> key = entry.getKey();
			Integer index = entry.getValue();
			Object value = scopedInstances[scopedInstances.length - 1].get(index);
			if (value != null) {
				result.put(key, value);
			}
		}
		return result;
	}

	public Set<Key<?>> getBindings() {
		return compiledIndexes.keySet();
	}

	public Set<Key<?>> getAllBindings() {
		return compiledBindings.keySet();
	}

	public <T> void putInstance(Class<T> key, T instance) {
		putInstance(Key.of(key), instance);
	}

	public <T> void putInstance(Key<T> key, T instance) {
		Integer index = compiledIndexes.get(key);
		if (index == null)
			throw new IllegalArgumentException("Key " + key + " is not found in scope " + Arrays.toString(getScope()));
		//noinspection unchecked
		scopedInstances[scopedInstances.length - 1].set(index, instance);
	}

	/**
	 * This method triggers creation of all keys that were marked as {@link EagerSingleton eager singletons}.
	 *
	 * @see EagerSingleton
	 */
	public Set<Key<?>> createEagerSingletons() {
		Set<Key<?>> eagerSingletons = getInstanceOr(new Key<Set<Key<?>>>(EagerSingleton.class) {}, emptySet());
		eagerSingletons.forEach(this::getInstanceOrNull); // orNull because bindings for some keys could be provided in scopes
		return eagerSingletons;
	}

	/**
	 * The key of type Set&lt;InstanceInjector&lt;?&gt;&gt; (note the wildcard type) is treated specially by this method,
	 * it calls all of the instance injectors the set contains on instances of their respective keys, if such instances
	 * were already made by this injector.
	 *
	 * @see AbstractModule#postInjectInto(Key)
	 */
	@SuppressWarnings({"unchecked", "JavadocReference"})
	public Set<Key<?>> postInjectInstances() {
		Set<InstanceInjector<?>> postInjectors = getInstanceOr(INSTANCE_INJECTORS_KEY, emptySet());
		for (InstanceInjector<?> instanceInjector : postInjectors) {
			Object instance = peekInstance(instanceInjector.key());
			if (instance != null) {
				((InstanceInjector<Object>) instanceInjector).injectInto(instance);
			}
		}
		return postInjectors.stream().map(InstanceInjector::key).collect(toSet());
	}

	@Nullable
	public Injector getParent() {
		return parent;
	}

	/**
	 * Returns the scope this injector operates upon.
	 * Scopes can be nested and this method returns a path
	 * for the binding graph trie as an array of trie prefixes.
	 */
	public Scope[] getScope() {
		return scopeTree.get().scope;
	}

	public Trie<Scope, Map<Key<?>, Binding<?>>> getBindingsTrie() {
		return scopeTree.map(graph -> graph.bindings);
	}

	@Nullable
	public <T> Binding<T> getBinding(Class<T> type) {
		return getBinding(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Key<T> key) {
		return (Binding<T>) getBindingsTrie().get().get(key);
	}

	public boolean hasBinding(Class<?> type) {
		return hasBinding(Key.of(type));
	}

	public boolean hasBinding(Key<?> key) {
		return compiledIndexes.containsKey(key);
	}

	/**
	 * Creates an injector that operates on a binding graph at a given prefix (scope) of the binding graph trie and this injector as its parent.
	 */
	public Injector enterScope(@NotNull Scope scope) {
		return new Injector(this, scopeTree.get(scope));
	}

}
