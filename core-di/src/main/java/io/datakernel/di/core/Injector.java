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
import io.datakernel.di.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static io.datakernel.di.core.BindingGenerator.REFUSING;
import static io.datakernel.di.core.BindingGenerator.combinedGenerator;
import static io.datakernel.di.core.BindingTransformer.IDENTITY;
import static io.datakernel.di.core.BindingTransformer.combinedTransformer;
import static io.datakernel.di.core.Multibinder.ERROR_ON_DUPLICATE;
import static io.datakernel.di.core.Multibinder.combinedMultibinder;
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

	@Nullable
	private final Injector parent;

	private static final class DependencyGraph {
		private final Scope[] scope;
		private final Trie<Scope, Map<Key<?>, Binding<?>>> bindingsTrie;
		private final Map<Key<?>, CompiledBinding<?>> compiledBindings;
		private final Map<Key<?>, Integer> instanceIndexes;

		private DependencyGraph(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindingsTrie, Map<Key<?>, CompiledBinding<?>> compiledBindings, Map<Key<?>, Integer> indexes) {
			this.scope = scope;
			this.bindingsTrie = bindingsTrie;
			this.compiledBindings = compiledBindings;
			instanceIndexes = indexes;
		}
	}

	private final Trie<Scope, DependencyGraph> scopeTree;
	private final Map<Key<?>, CompiledBinding<?>> compiledBindings;
	private final Map<Key<?>, Integer> compiledIndexes;
	private final AtomicReferenceArray[] instances;

	private static final Object[] NO_OBJECTS = new Object[0];
	private static final Object NO_KEY = new Object();

	private Injector(@Nullable Injector parent, Trie<Scope, DependencyGraph> scopeTree) {
		this.parent = parent;
		this.scopeTree = scopeTree;
		this.instances = parent == null ? new AtomicReferenceArray[1] : Arrays.copyOf(parent.instances, parent.instances.length + 1);
		this.instances[this.instances.length - 1] = new AtomicReferenceArray(scopeTree.get().instanceIndexes.size());
		this.compiledBindings = scopeTree.get().compiledBindings;
		this.compiledIndexes = scopeTree.get().instanceIndexes;
	}

	/**
	 * This constructor combines given modules (along with a {@link DefaultModule})
	 * and then {@link #compile(Module) compiles} them.
	 */
	public static Injector of(Module... modules) {
		return compile(Modules.combine(Modules.combine(modules), new DefaultModule()));
	}

	/**
	 * This constructor is a shortcut for threadsafe {@link #compile(Injector, Scope[], Trie, Multibinder, BindingTransformer, BindingGenerator) compile}
	 * with no instance overrides and no multibinders, transformers or generators.
	 */
	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return compile(null, UNSCOPED, bindings.map(Utils::toMultimap), ERROR_ON_DUPLICATE, IDENTITY, REFUSING);
	}

	/**
	 * This constructor threadsafely {@link #compile(Injector, Scope[], Trie, Multibinder, BindingTransformer, BindingGenerator) compiles}
	 * given module, extracting bindings and their multibinders, transformers and generators from it, with no instance overrides
	 */
	public static Injector compile(Module module) {
		return compile(null, UNSCOPED, module.getBindings(),
				combinedMultibinder(module.getMultibinders()),
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
	 * @param multibinder      a multibindinder that is called on every binding conflict (see {@link Multibinder#combinedMultibinder})
	 * @param transformer      a transformer that is called on every binding once (see {@link BindingTransformer#combinedTransformer})
	 * @param generator        a generator that is called on every missing binding (see {@link BindingGenerator#combinedGenerator})
	 * @see #enterScope
	 */
	public static Injector compile(@Nullable Injector parent,
			Scope[] scope,
			@NotNull Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindingsMultimap,
			@NotNull Multibinder<?> multibinder,
			@NotNull BindingTransformer<?> transformer,
			@NotNull BindingGenerator<?> generator) {

		Trie<Scope, Map<Key<?>, Binding<?>>> bindings = Preprocessor.resolveConflicts(bindingsMultimap, multibinder);

		Injector[] injectorRef = new Injector[1];

		// well, can't do anything better than that
		bindings.get().put(Key.of(Injector.class), Binding.to(() -> injectorRef[0]));

		Preprocessor.completeBindingGraph(bindings, transformer, generator);

		Map<Key<?>, Set<Map.Entry<Key<?>, Binding<?>>>> unsatisfied = Preprocessor.getUnsatisfiedDependencies(bindings);
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
					.map(cycle -> {
						int offset = Utils.getKeyDisplayCenter(cycle[0]);
						String cycleString = Arrays.stream(cycle).map(Key::getDisplayString).collect(joining(" -> ", "\t", ""));
						String indent = new String(new char[offset]).replace('\0', ' ');
						String line = new String(new char[cycleString.length() - offset]).replace('\0', '-');
						return cycleString + " -,\n\t" + indent + "^" + line + "'";
					})
					.collect(joining("\n\n", "Cyclic dependencies detected:\n\n", "\n")));
		}

		Injector injector = new Injector(null, compileBindingsTrie(UNSCOPED, bindings, emptyMap()));
		injectorRef[0] = injector;

		return injector;
	}

	protected static Trie<Scope, DependencyGraph> compileBindingsTrie(Scope[] path, Trie<Scope, Map<Key<?>, Binding<?>>> bindingsTrie,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsParent) {
		DependencyGraph dependencyGraph = compileBindings(path, bindingsTrie, compiledBindingsParent);
		Map<Scope, Trie<Scope, DependencyGraph>> children = new HashMap<>();
		bindingsTrie.getChildren().forEach((childScope, trie) -> {
			Map<Key<?>, CompiledBinding<?>> compiledBindingsCopy = new HashMap<>(compiledBindingsParent);
			compiledBindingsCopy.putAll(dependencyGraph.compiledBindings);
			children.put(childScope,
					compileBindingsTrie(next(path, childScope), bindingsTrie.get(childScope), compiledBindingsCopy));
		});
		return new Trie<>(dependencyGraph, children);
	}

	protected static DependencyGraph compileBindings(Scope[] path, Trie<Scope, Map<Key<?>, Binding<?>>> bindingsTrie,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsParent) {
		Map<Key<?>, Binding<?>> bindings = bindingsTrie.get();
		Map<Key<?>, CompiledBinding<?>> compiledBindings = new HashMap<>();
		Map<Key<?>, Integer> instanceIndexes = new HashMap<>();
		for (Key<?> key : bindings.keySet()) {
			compiledBindings.put(key,
					compileBinding(path.length, key, bindings, compiledBindingsParent, compiledBindings, instanceIndexes));
		}
		compiledBindingsParent.forEach(compiledBindings::putIfAbsent);
		return new DependencyGraph(path, bindingsTrie, compiledBindings, instanceIndexes);
	}

	private static CompiledBinding<?> compileBinding(int level, Key<?> key,
			Map<Key<?>, Binding<?>> bindings,
			Map<Key<?>, CompiledBinding<?>> compiledBindingsParent,
			Map<Key<?>, CompiledBinding<?>> compiledBindings,
			Map<Key<?>, Integer> instanceIndexes) {
		if (compiledBindings.containsKey(key)) return compiledBindings.get(key);
		if (compiledBindingsParent.containsKey(key)) return compiledBindingsParent.get(key);
		Binding<?> binding = bindings.get(key);
		if (binding == null) return missingOptionalBinding();
		int index = instanceIndexes.size();
		instanceIndexes.put(key, index);
		CompiledBinding<?> compiledBinding = binding.getCompiler().compile(
				new CompiledBindingLocator() {
					@Override
					public @NotNull <Q> CompiledBinding<Q> locate(Key<Q> key) {
						//noinspection unchecked
						return (CompiledBinding<Q>) compileBinding(level, key, bindings, compiledBindingsParent,
								compiledBindings, instanceIndexes);
					}
				},
				level, index);
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
			return (T) binding.getInstance(instances);
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
		return binding != null ? (T) binding.getInstance(instances) : null;
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
			return (T) binding.createInstance(instances);
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
		return binding != null ? (T) binding.createInstance(instances) : null;
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
		return (T) instances[instances.length - 1].get(index);
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
		return instances[instances.length - 1].get(index) != null;
	}

	/**
	 * This method returns a copy of the injector cache - a map of all already created instances.
	 */
	public Map<Key<?>, Object> peekInstances() {
		Map<Key<?>, Object> result = new HashMap<>();
		for (Map.Entry<Key<?>, Integer> entry : compiledIndexes.entrySet()) {
			Key<?> key = entry.getKey();
			Integer index = entry.getValue();
			Object value = instances[instances.length - 1].get(index);
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

	public <T> void putInstance(Key<T> key, T instance) {
		Integer index = compiledIndexes.get(key);
		if (index == null)
			throw new IllegalArgumentException("Key " + key + " is not found in scope " + Arrays.toString(getScope()));
		//noinspection unchecked
		instances[instances.length - 1].set(index, instance);
	}

	/**
	 * This method triggers creation of all keys that were marked as {@link EagerSingleton eager singletons}.
	 *
	 * @see EagerSingleton
	 */
	public Set<Key<?>> createEagerSingletons() {
		Set<Key<?>> eagerSingletons = getInstanceOr(new Key<Set<Key<?>>>(EagerSingleton.class) {}, emptySet());
		eagerSingletons.forEach(this::getInstance);
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
		return scopeTree.get().bindingsTrie;
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
