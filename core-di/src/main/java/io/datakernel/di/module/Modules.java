package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import io.datakernel.di.util.Trie;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;

public final class Modules {
	private Modules() {
	}

	public static Module of(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return new ModuleImpl(bindings.map(map ->
				map.entrySet().stream()
						.collect(toMap(Map.Entry::getKey, entry -> singleton(entry.getValue())))), emptyMap(), emptyMap(), emptyMap());
	}

	public static Module combine(Module... modules) {
		return modules.length == 1 ? modules[0] : combine(Arrays.asList(modules));
	}

	public static Module combine(Collection<Module> modules) {
		if (modules.size() == 1) {
			return modules.iterator().next();
		}
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(multimapMerger(), new HashMap<>(), modules.stream().map(Module::getBindingsMultimap));

		Map<Integer, BindingTransformer<?>> bindingTransformers = new HashMap<>();
		Map<Type, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
		Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

		for (Module module : modules) {
			mergeBindingTransformers(bindingTransformers, module.getBindingTransformers());
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
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(Map::putAll, new HashMap<>(), into.getBindingsMultimap(), replacements.getBindingsMultimap());

		Map<Integer, BindingTransformer<?>> bindingTransformers = new HashMap<>(into.getBindingTransformers());
		bindingTransformers.putAll(replacements.getBindingTransformers());

		Map<Type, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>(into.getBindingGenerators());
		bindingGenerators.putAll(replacements.getBindingGenerators());

		Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>(into.getMultibinders());
		multibinders.putAll(replacements.getMultibinders());

		return new ModuleImpl(bindings, bindingTransformers, bindingGenerators, multibinders);
	}

	private static final Function<Set<Binding<?>>, Binding<?>> ERRORS_ON_DUPLICATE = bindings -> {
		throw new IllegalArgumentException();
	};

	@SuppressWarnings("unchecked")
	public static <T> Function<Set<Binding<T>>, Binding<T>> getErrorsOnDuplicate() {
		return (Function) ERRORS_ON_DUPLICATE;
	}

	private static class ModuleImpl implements Module {
		private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings;
		private final Map<Integer, BindingTransformer<?>> bindingMappers;
		private final Map<Type, Set<BindingGenerator<?>>> bindingGenerators;
		private final Map<Key<?>, Multibinder<?>> multibinders;

		private ModuleImpl(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
				Map<Integer, BindingTransformer<?>> bindingMappers,
				Map<Type, Set<BindingGenerator<?>>> bindingGenerators,
				Map<Key<?>, Multibinder<?>> multibinders) {
			this.bindings = bindings;
			this.bindingMappers = bindingMappers;
			this.bindingGenerators = bindingGenerators;
			this.multibinders = multibinders;
		}

		@Override
		public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
			return bindings;
		}

		@Override
		public Map<Integer, BindingTransformer<?>> getBindingTransformers() {
			return bindingMappers;
		}

		@Override
		public Map<Type, Set<BindingGenerator<?>>> getBindingGenerators() {
			return bindingGenerators;
		}

		@Override
		public Map<Key<?>, Multibinder<?>> getMultibinders() {
			return multibinders;
		}
	}
}
