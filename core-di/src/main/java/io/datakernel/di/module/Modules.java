package io.datakernel.di.module;

import io.datakernel.di.core.*;
import io.datakernel.di.impl.BindingLocator;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;

import static io.datakernel.di.core.Name.uniqueName;
import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptyMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

/**
 * This class contains a set of utilities for working with {@link Module modules}.
 */
public final class Modules {
	static Module EMPTY = new SimpleModule(Trie.leaf(emptyMap()), emptyMap(), emptyMap(), emptyMap());

	private Modules() {
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

		return new SimpleModule(bindings, bindingTransformers, bindingGenerators, multibinders);
	}

	/**
	 * @see #combine(Collection)
	 */
	public static Module combine(Module... modules) {
		return modules.length == 0 ? Module.empty() : modules.length == 1 ? modules[0] : combine(Arrays.asList(modules));
	}

	/**
	 * Consecutively overrides each of the given modules with the next one after it and returns the accumulated result.
	 */
	public static Module override(List<Module> modules) {
		return modules.stream().reduce(Module.empty(), Modules::override);
	}

	/**
	 * @see #combine(Collection)
	 */
	public static Module override(Module... modules) {
		return override(Arrays.asList(modules));
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

		return new SimpleModule(bindings, bindingTransformers, bindingGenerators, multibinders);
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
		return new SimpleModule(Trie.leaf(bindings), from.getBindingTransformers(), from.getBindingGenerators(), from.getMultibinders());
	}

	static Module export(Module module, Set<Key<?>> exportedKeys) {
		Set<Key<?>> originalKeys = new HashSet<>();
		module.getBindings().dfs(multimap -> originalKeys.addAll(multimap.keySet()));

		Set<Key<?>> missing = new HashSet<>(exportedKeys);
		missing.removeAll(originalKeys);

		if (!missing.isEmpty()) {
			throw new DIException(missing.stream()
					.map(Key::getDisplayString)
					.collect(joining(", ", "Exporting keys ", " that were not provided by the module")));
		}

		return doRebind(module,
				originalKeys.stream()
						.filter(originalKey -> !exportedKeys.contains(originalKey))
						.collect(toMap(identity(), originalKey ->
								Key.ofType(originalKey.getType(), uniqueName(originalKey.getName())))));
	}

	static Module rebindExports(Module module, Map<Key<?>, Key<?>> originalToNew) {
		Set<Key<?>> originalKeys = new HashSet<>();

		module.getBindings().dfs(multimap -> originalKeys.addAll(multimap.keySet()));

		if (originalToNew.keySet().stream().noneMatch(originalKeys::contains)) {
			return module;
		}
		return doRebind(module, originalToNew);
	}

	@SuppressWarnings("unchecked")
	static Module rebindImports(Module module, Map<Key<?>, Binding<?>> rebinds) {
		Map<Key<?>, Key<?>> originalToNew = new HashMap<>();

		rebinds.forEach((k, b) -> {
			Key<Object> priv = (Key<Object>) k.named(uniqueName(k.getName()));
			originalToNew.put(k, priv);
		});

		Module renamed = doRebind(module, originalToNew);
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = renamed.getBindings();
		Map<Key<?>, Set<Binding<?>>> localBindings = new HashMap<>(bindings.get());

		rebinds.forEach((k, b) -> {
			Set<Binding<?>> bindingSet = localBindings.computeIfAbsent(originalToNew.get(k), $ -> new HashSet<>());
			bindingSet.clear();
			bindingSet.add(b);
		});

		return Module.of(Trie.of(localBindings, bindings.getChildren()), renamed.getBindingTransformers(), renamed.getBindingGenerators(), renamed.getMultibinders());
	}

	static Module rebindImportKeys(Module module, Map<Key<?>, Key<?>> mapping) {
		return rebindImports(module, mapping.entrySet().stream().collect(toMap(Entry::getKey, e -> Binding.to(e.getValue()))));
	}

	@SuppressWarnings("unchecked")
	static Module rebindImports(Module module, BiFunction<Key<?>, Binding<?>, Binding<?>> rebinder) {
		return new SimpleModule(
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
									return generated != null ? (Binding<Object>) rebinder.apply(key, generated) : null;
								}),
				module.getMultibinders());
	}

	private static <T> Binding<T> rebindMatchedDependencies(Binding<T> binding, Map<Key<?>, Key<?>> originalToNew) {
		return binding.rebindDependencies(
				binding.getDependencies()
						.stream()
						.map(Dependency::getKey)
						.filter(originalToNew::containsKey)
						.collect(toMap(identity(), originalToNew::get)));
	}

	@SuppressWarnings("unchecked")
	private static Module doRebind(Module module, Map<Key<?>, Key<?>> originalToNew) {
		Map<Key<?>, Key<?>> newToOriginal = originalToNew.entrySet().stream().collect(toMap(Entry::getValue, Entry::getKey));
		return new SimpleModule(
				module.getBindings()
						.map(bindingsMap ->
								transformMultimap(bindingsMap,
										key -> originalToNew.getOrDefault(key, key),
										(key, binding) -> {
											if (isKeySet(key)) {
												binding = binding.mapInstance(set ->
														((Set<Key<Object>>) set).stream()
																.map(k -> originalToNew.getOrDefault(k, k))
																.collect(toSet()));
											}
											return rebindMatchedDependencies(binding, originalToNew);
										})),
				transformMultimapValues(module.getBindingTransformers(),
						($, transformer) ->
								(bindings, scope, key, binding) -> {
									Binding<Object> transformed = ((BindingTransformer<Object>) transformer).transform(
											new BindingLocator() {
												@Override
												public @Nullable <T> Binding<T> get(Key<T> key) {
													return (Binding<T>) bindings.get(originalToNew.getOrDefault(key, key));
												}
											},
											scope,
											(Key<Object>) newToOriginal.getOrDefault(key, key),
											binding);
									return transformed.equals(binding) ?
											binding :
											rebindMatchedDependencies(transformed, originalToNew);
								}),
				transformMultimapValues(module.getBindingGenerators(),
						($, generator) ->
								(bindings, scope, key) -> {
									Binding<Object> binding = ((BindingGenerator<Object>) generator).generate(
											new BindingLocator() {
												@Override
												public @Nullable <T> Binding<T> get(Key<T> key) {
													return (Binding<T>) bindings.get(originalToNew.getOrDefault(key, key));
												}
											},
											scope,
											(Key<Object>) newToOriginal.getOrDefault(key, key));
									return binding != null ?
											rebindMatchedDependencies(binding, originalToNew) :
											null;
								}),
				module.getMultibinders()
						.entrySet()
						.stream()
						.collect(toMap(e -> originalToNew.getOrDefault(e.getKey(), e.getKey()), e -> {
							Multibinder<?> value = e.getValue();
							if (!originalToNew.containsKey(e.getKey())) {
								return value;
							}
							return (key, bindings) -> rebindMatchedDependencies(((Multibinder<Object>) value).multibind(key, bindings), originalToNew);
						})));
	}
}
