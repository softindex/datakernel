package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptyMap;

/**
 * This class contains a set of utilities for working with {@link Module modules}.
 */
public final class Modules {
	private Modules() {
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie
	 */
	public static Module of(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return new ModuleImpl(bindings.map(Utils::toMultimap), emptyMap(), emptyMap(), emptyMap());
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie, transformers, generators and multibinders
	 */
	public static Module of(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
			Map<Integer, Set<BindingTransformer<?>>> transformers,
			Map<Class<?>, Set<BindingGenerator<?>>> generators,
			Map<Key<?>, Multibinder<?>> multibinders) {
		return new ModuleImpl(bindings, transformers, generators, multibinders);
	}

	/**
	 * @see #combine(Collection)
	 */
	public static Module combine(Module... modules) {
		return modules.length == 1 ? modules[0] : combine(Arrays.asList(modules));
	}

	/**
	 * Combines multiple modules into one.
	 */
	public static Module combine(Collection<Module> modules) {
		if (modules.size() == 1) {
			return modules.iterator().next();
		}
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(multimapMerger(), new HashMap<>(), modules.stream().map(Module::getBindings));

		Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
		Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
		Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

		for (Module module : modules) {
			combineMultimap(bindingTransformers, module.getBindingTransformers());
			combineMultimap(bindingGenerators, module.getBindingGenerators());
			mergeMultibinders(multibinders, module.getMultibinders());
		}

		return new ModuleImpl(bindings, bindingTransformers, bindingGenerators, multibinders);
	}

	public static Module override(Module... modules) {
		return override(Arrays.asList(modules));
	}

	public static Module override(List<Module> modules) {
		return modules.stream().reduce(Module.empty(), Modules::override);
	}

	/**
	 * This method creates a module that has bindings, transformers, generators and multibinders from first module
	 * replaced with bindings, transformers, generators and multibinders from the second module.
	 */
	public static Module override(Module into, Module replacements) {
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(Map::putAll, new HashMap<>(), into.getBindings(), replacements.getBindings());

		Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>(into.getBindingTransformers());
		bindingTransformers.putAll(replacements.getBindingTransformers());

		Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>(into.getBindingGenerators());
		bindingGenerators.putAll(replacements.getBindingGenerators());

		Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>(into.getMultibinders());
		multibinders.putAll(replacements.getMultibinders());

		return new ModuleImpl(bindings, bindingTransformers, bindingGenerators, multibinders);
	}

	/**
	 * Creates a module with all trie nodes merged into one and placed at root.
	 * Basically, any scopes are ignored.
	 * This is useful for some tests.
	 */
	public static Module ignoreScopes(Module from) {
		Map<Key<?>, Set<Binding<?>>> bindings = new HashMap<>();
		Map<Key<?>, Scope[]> scopes = new HashMap<>();
		from.getBindings().dfs(UNSCOPED, (scope, localBindings) ->
				localBindings.forEach((k, b) -> {
					bindings.merge(k, b, ($, $2) -> {
						Scope[] alreadyThere = scopes.get(k);
						String where = alreadyThere.length == 0 ? "in root" : "in scope " + getScopeDisplayString(alreadyThere);
						throw new IllegalStateException("Duplicate key " + k + ", already defined " + where + " and in scope " + getScopeDisplayString(scope));
					});
					scopes.put(k, scope);
				}));
		return new ModuleImpl(Trie.leaf(bindings), from.getBindingTransformers(), from.getBindingGenerators(), from.getMultibinders());
	}

	public static class ModuleImpl implements Module {
		private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings;
		private final Map<Integer, Set<BindingTransformer<?>>> transformers;
		private final Map<Class<?>, Set<BindingGenerator<?>>> generators;
		private final Map<Key<?>, Multibinder<?>> multibinders;

		private ModuleImpl(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
				Map<Integer, Set<BindingTransformer<?>>> transformers,
				Map<Class<?>, Set<BindingGenerator<?>>> generators,
				Map<Key<?>, Multibinder<?>> multibinders) {
			this.bindings = bindings;
			this.transformers = transformers;
			this.generators = generators;
			this.multibinders = multibinders;
		}

		@Override
		public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
			return bindings;
		}

		@Override
		public Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
			return transformers;
		}

		@Override
		public Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
			return generators;
		}

		@Override
		public Map<Key<?>, Multibinder<?>> getMultibinders() {
			return multibinders;
		}
	}

	@SuppressWarnings("unchecked")
	public static Module rebind(Module module, BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return new ModuleImpl(
				module.getBindings().map(bindingsMap -> transformMultimapValues(bindingsMap, rebinder)),
				transformMultimapValues(module.getBindingTransformers(),
						(priority, bindingTransformer) ->
								(bindings, scope, key, binding) -> {
									Binding<Object> transformed = ((BindingTransformer<Object>) bindingTransformer).transform(bindings, scope, key, binding);
									if (transformed == binding) return binding;
									return (Binding<Object>) rebinder.apply(key, transformed);
								}),
				transformMultimapValues(module.getBindingGenerators(),
						(clazz, bindingGenerator) ->
								(provider, scope, key) -> {
									Binding<Object> generated = ((BindingGenerator<Object>) bindingGenerator).generate(provider, scope, key);
									if (generated == null) return null;
									return (Binding<Object>) rebinder.apply(key, generated);
								}),
				module.getMultibinders());
	}

	@SafeVarargs
	public static BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(BiFunction<Key<?>, Binding<?>, Binding<?>>... rebinders) {
		return rebinder(Arrays.asList(rebinders));
	}

	public static BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(List<BiFunction<Key<?>, Binding<?>, Binding<?>>> rebinders) {
		return rebinders.stream().reduce(
				(key, binding) -> binding,
				(fn1, fn2) -> (key, binding) -> fn2.apply(key, fn1.apply(key, binding)));
	}

	public static <T, V> BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(Key<T> componentKey, Key<V> from, Key<? extends V> to) {
		return (k, binding) -> {
			if (!componentKey.equals(k)) return binding;
			return binding.rebindDependency(from, to);
		};
	}

	public static <T, V> BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(Key<T> componentKey, Function<Binding<V>, Binding<? extends V>> fn) {
		return (k, binding) -> {
			if (!componentKey.equals(k)) return binding;
			//noinspection unchecked
			return fn.apply((Binding<V>) binding);
		};
	}

	public static <V> BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(Key<V> from, Key<? extends V> to) {
		return (k, binding) ->
				binding.getDependencies().stream().map(Dependency::getKey).anyMatch(Predicate.isEqual(from)) ?
						binding.rebindDependency(from, to) :
						binding;
	}

}
