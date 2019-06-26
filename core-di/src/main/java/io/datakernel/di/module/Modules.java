package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;

import java.util.*;
import java.util.function.BiFunction;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptyMap;

@SuppressWarnings("WeakerAccess")
public final class Modules {
	private Modules() {
	}

	public static Module of(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return new ModuleImpl(bindings.map(Utils::toMultimap), emptyMap(), emptyMap(), emptyMap());
	}

	public static Module of(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
			Map<Integer, Set<BindingTransformer<?>>> transformers,
			Map<Class<?>, Set<BindingGenerator<?>>> generators,
			Map<Key<?>, Multibinder<?>> multibinders) {
		return new ModuleImpl(bindings, transformers, generators, multibinders);
	}

	public static Module combine(Module... modules) {
		return modules.length == 1 ? modules[0] : combine(Arrays.asList(modules));
	}

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

		public ModuleImpl(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
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
								(provider, scope, key, binding) -> {
									Binding<Object> transformed = ((BindingTransformer<Object>) bindingTransformer).transform(provider, scope, key, binding);
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
	public static BiFunction<Key<?>, Binding<?>, Binding<?>> rebinders(BiFunction<Key<?>, Binding<?>, Binding<?>>... rebinders) {
		return rebinders(Arrays.asList(rebinders));
	}

	public static BiFunction<Key<?>, Binding<?>, Binding<?>> rebinders(List<BiFunction<Key<?>, Binding<?>, Binding<?>>> rebinders) {
		return rebinders.stream().reduce(
				(key, binding) -> binding,
				(fn1, fn2) -> (key, binding) -> fn2.apply(key, fn1.apply(key, binding)));
	}

	public static <T, V> BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(Key<T> key, Key<V> from, Key<? extends V> to) {
		return rebinder(key, from, new Dependency(to));
	}

	public static <T, V> BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(Key<T> key, Key<V> from, Dependency to) {
		return (k, binding) -> {
			if (!key.equals(k)) return binding;
			return binding.rebindDependency(from, to);
		};
	}

	public static <V> BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(Key<V> from, Key<? extends V> to) {
		return rebinder(from, new Dependency(to));
	}

	public static <T, V> BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder(Key<V> from, Dependency to) {
		return (k, binding) -> binding.rebindDependency(from, to);
	}

}
