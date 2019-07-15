package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;

import java.util.*;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptyMap;

/**
 * This class contains a set of utilities for working with {@link Module modules}.
 */
@SuppressWarnings("WeakerAccess")
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

}
