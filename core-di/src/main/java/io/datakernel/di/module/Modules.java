package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Dependency;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import io.datakernel.di.util.Constructors.Factory;
import io.datakernel.di.util.Trie;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

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

	public static <T> Multibinder<T> resolverOfReducer(Function<Stream<T>, T> reducerFunction) {
		return bindings -> {
			if (bindings.size() == 1) {
				return bindings.iterator().next();
			}
			List<Factory<T>> factories = new ArrayList<>();
			List<Dependency> dependencies = new ArrayList<>();
			for (Binding<T> binding : bindings) {
				int from = dependencies.size();
				int to = from + binding.getDependencies().length;
				Collections.addAll(dependencies, binding.getDependencies());
				factories.add(args -> binding.getFactory().create(Arrays.copyOfRange(args, from, to)));
			}
			return Binding.of(
					args -> reducerFunction.apply(factories.stream().map(factory -> factory.create(args))),
					dependencies.toArray(new Dependency[0]));
		};
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	public static <T> Multibinder<T> resolverOfBinaryOperator(BinaryOperator<T> binaryOperator) {
		return resolverOfReducer(stream -> stream.reduce(binaryOperator).get());
	}

	private static final Multibinder<Set<Object>> MULTIBINDER_TO_SET = resolverOfReducer(stream -> {
		Set<Object> result = new HashSet<>();
		stream.forEach(result::addAll);
		return result;
	});

	private static final Multibinder<Map<Object, Object>> MULTIBINDER_TO_MAP = resolverOfReducer(stream -> {
		Map<Object, Object> result = new HashMap<>();
		stream.forEach(map ->
				map.forEach((k, v) ->
						result.merge(k, v, ($, $2) -> {
							throw new IllegalStateException("Duplicate key " + k);
						})));
		return result;
	});

	@SuppressWarnings("unchecked")
	public static <T> Multibinder<Set<T>> multibinderToSet() {
		return (Multibinder) MULTIBINDER_TO_SET;
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Multibinder<Map<K, V>> multibinderToMap() {
		return (Multibinder) MULTIBINDER_TO_MAP;
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
