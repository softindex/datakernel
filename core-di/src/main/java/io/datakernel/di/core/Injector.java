package io.datakernel.di.core;

import io.datakernel.di.annotation.EagerSingleton;
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

import static io.datakernel.di.core.BindingGenerator.REFUSING;
import static io.datakernel.di.core.BindingGenerator.combinedGenerator;
import static io.datakernel.di.core.BindingTransformer.IDENTITY;
import static io.datakernel.di.core.BindingTransformer.combinedTransformer;
import static io.datakernel.di.core.Multibinder.ERROR_ON_DUPLICATE;
import static io.datakernel.di.core.Multibinder.combinedMultibinder;
import static io.datakernel.di.core.Scope.UNSCOPED;
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
public class Injector {
	@Nullable
	private final Injector parent;
	private final Scope[] scope;

	private final Trie<Scope, Map<Key<?>, Binding<?>>> bindings;
	private final Trie<Scope, Map<Key<?>, Binding<?>>> localBindings;
	private final Map<Key<?>, Binding<?>> bindingGraph;
	private final Map<Key<?>, Object> instances;

	protected static final class SynchronizedInjector extends Injector {
		protected SynchronizedInjector(@Nullable Injector parent, Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindings, Map<Key<?>, Object> instances) {
			super(parent, scope, bindings, instances);
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

		@Override
		public boolean isThreadSafe() {
			return true;
		}
	}

	private static final Object[] NO_OBJECTS = new Object[0];
	private static final Object NO_KEY = new Object();

	private Injector(@Nullable Injector parent, Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindings, Map<Key<?>, Object> instances) {
		this.parent = parent;
		this.scope = scope;
		this.bindings = bindings;
		this.instances = instances;

		Trie<Scope, Map<Key<?>, Binding<?>>> localBindings = bindings.get(scope);
		this.localBindings = localBindings;
		this.bindingGraph = localBindings != null ? localBindings.get() : emptyMap();
	}

	/**
	 * This constructor combines given modules (along with a {@link DefaultModule})
	 * and then {@link #compile(Module) compiles} them.
	 */
	public static Injector of(Module... modules) {
		return compile(Modules.combine(Modules.combine(modules), new DefaultModule()));
	}

	/**
	 * This constructor is a shortcut for threadsafe {@link #compile(Injector, Map, boolean, Scope[], Trie, Multibinder, BindingTransformer, BindingGenerator) compile}
	 * with no instance overrides and no multibinders, transformers or generators.
	 */
	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return compile(null, new HashMap<>(), true, UNSCOPED, bindings.map(Utils::toMultimap), ERROR_ON_DUPLICATE, IDENTITY, REFUSING);
	}

	/**
	 * This constructor threadsafely {@link #compile(Injector, Map, boolean, Scope[], Trie, Multibinder, BindingTransformer, BindingGenerator) compiles}
	 * given module, extracting bindings and their multibinders, transformers and generators from it, with no instance overrides
	 */
	public static Injector compile(Module module) {
		return compile(null, new HashMap<>(), true, UNSCOPED, module.getBindings(),
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
	 * @param instances        instance overrides - preemptively created instanced for certain keys,
	 *                         may be used to override what a respective binding would otherwise create.
	 * @param threadsafe       should each method of the resulting injector be synchronized
	 * @param scope            the scope of the injector, can be described as 'prefix of the root' of the binding trie,
	 *                         used when {@link #enterScope entering scopes}
	 * @param bindingsMultimap a trie of binding set graph with multiple possible conflicting bindings per key
	 *                         that are resolved as part of the compilation.
	 * @param multibinder      a multibindinder that is called on every binding conflict (see {@link Multibinder#combinedMultibinder})
	 * @param transformer      a transformer that is called on every binding once (see {@link BindingTransformer#combinedTransformer})
	 * @param generator        a generator that is called on every missing binding (see {@link BindingGenerator#combinedGenerator})
	 *
	 * @see #enterScope
	 */
	public static Injector compile(@Nullable Injector parent, Map<Key<?>, Object> instances, boolean threadsafe,
			Scope[] scope,
			@NotNull Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindingsMultimap,
			@NotNull Multibinder<?> multibinder,
			@NotNull BindingTransformer<?> transformer,
			@NotNull BindingGenerator<?> generator) {

		Trie<Scope, Map<Key<?>, Binding<?>>> bindings = BindingGraph.resolveConflicts(bindingsMultimap, multibinder);

		Injector injector = threadsafe ?
				new SynchronizedInjector(parent, scope, bindings, instances) :
				new Injector(parent, scope, bindings, instances);

		// well, can't do anything better than that
		bindings.get().put(Key.of(Injector.class), Binding.toInstance(injector));

		BindingGraph.completeBindingGraph(bindings, transformer, generator);

		Map<Key<?>, Set<Map.Entry<Key<?>, Binding<?>>>> unsatisfied = BindingGraph.getUnsatisfiedDependencies(bindings);
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

		Set<Key<?>[]> cycles = BindingGraph.getCyclicDependencies(bindings);
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
			throw cannotConstruct(key, bindingGraph.get(key));
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
			throw cannotConstruct(key, bindingGraph.get(key));
		}
		return instance;
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
		Binding<?> binding = bindingGraph.get(key);
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
					throw cannotConstruct(dependencyKey, bindingGraph.get(dependencyKey));
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

	private static DIException cannotConstruct(Key<?> key, @Nullable Binding<?> binding) {
		return new DIException((binding != null ? "Binding refused to" : "No binding to") + " construct an instance for key " +
				key.getDisplayString() + (binding != null && binding.getLocation() != null ? ("\n\t at" + binding.getLocation()) : ""));
	}

	/**
	 * This method returns an instance only if it already was created by a {@link #getInstance} call before,
	 * it does not trigger instance creation.
	 */
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
		return (T) instances.get(key);
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
	public boolean hasInstance(@NotNull Key<?> type) {
		return instances.get(type) != null;
	}

	/**
	 * This method returns a copy of the injector cache - a map of all already created instances.
	 */
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

	/**
	 * This method triggers creation of all keys that were marked as {@link EagerSingleton eager singletons}.
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
	 * @see AbstractModule#postInjectInto
	 */
	@SuppressWarnings("unchecked")
	public Set<Key<?>> postInjectInstances() {
		Set<InstanceInjector<?>> postInjectors = getInstanceOr(new Key<Set<InstanceInjector<?>>>() {}, emptySet());
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
		return scope;
	}

	public Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		return localBindings;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Class<T> type) {
		return (Binding<T>) bindingGraph.get(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Key<T> key) {
		return (Binding<T>) bindingGraph.get(key);
	}

	public boolean hasBinding(Class<?> type) {
		return hasBinding(Key.of(type));
	}

	public boolean hasBinding(Key<?> key) {
		return bindingGraph.containsKey(key);
	}

	/**
	 * {@link #enterScope(Scope, Map, boolean) Enters} the scope with no instance overrides and same threadsafety as the current injector.
	 */
	public Injector enterScope(@NotNull Scope scope) {
		return enterScope(scope, new HashMap<>(), isThreadSafe());
	}

	/**
	 * Creates an injector that operates on a binding graph at a given prefix (scope) of the binding graph trie and this injector as its parent.
	 */
	public Injector enterScope(@NotNull Scope scope, @NotNull Map<Key<?>, Object> instances, boolean threadsafe) {
		Scope[] nextScope = next(this.scope, scope);
		return threadsafe ?
				new SynchronizedInjector(this, nextScope, bindings, instances) :
				new Injector(this, nextScope, bindings, instances);
	}

	public boolean isThreadSafe() {
		return false;
	}
}
