package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Dependency;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.di.util.Constructors.Factory;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.mergeConflictResolvers;
import static io.datakernel.di.util.Utils.multimapMerger;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;

public final class Modules {
	private Modules() {
	}

	public static Module of(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return new ModuleImpl(bindings.map(map ->
				map.entrySet().stream()
						.collect(toMap(Map.Entry::getKey, entry -> singleton(entry.getValue())))), emptyMap());
	}

	public static Module combine(Module... modules) {
		return modules.length == 1 ? modules[0] : combine(Arrays.asList(modules));
	}

	public static Module combine(Collection<Module> modules) {
		if (modules.size() == 1) {
			return modules.iterator().next();
		}
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(multimapMerger(), new HashMap<>(), modules.stream().map(Module::getBindingsMultimap));
		Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>();

		for (Module module : modules) {
			mergeConflictResolvers(conflictResolvers, module.getConflictResolvers());
		}

		return new ModuleImpl(bindings, conflictResolvers);
	}

	public static Module override(Module... modules) {
		return override(Arrays.asList(modules));
	}

	public static Module override(List<Module> modules) {
		return modules.stream().reduce(Module.empty(), Modules::override);
	}

	public static Module override(Module into, Module replacements) {
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(Map::putAll, new HashMap<>(), into.getBindingsMultimap(), replacements.getBindingsMultimap());

		Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>(into.getConflictResolvers());
		conflictResolvers.putAll(replacements.getConflictResolvers());
		return new ModuleImpl(bindings, conflictResolvers);
	}

	private static final Function<Set<Binding<?>>, Binding<?>> ERRORS_ON_DUPLICATE = bindings -> {
		throw new IllegalArgumentException();
	};

	@SuppressWarnings("unchecked")
	public static <T> Function<Set<Binding<T>>, Binding<T>> getErrorsOnDuplicate() {
		return (Function) ERRORS_ON_DUPLICATE;
	}

	public static <T> Function<Set<Binding<T>>, Binding<T>> resolverOfReducer(Function<Stream<T>, T> reducerFunction) {
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
			return Binding.of(dependencies.toArray(new Dependency[0]), args -> reducerFunction.apply(factories.stream().map(factory -> factory.create(args))));
		};
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	public static <T> Function<Set<Binding<T>>, Binding<T>> resolverOfBinaryOperator(BinaryOperator<T> binaryOperator) {
		return resolverOfReducer(stream -> stream.reduce(binaryOperator).get());
	}

	private static final Function<Set<Binding<Set<Object>>>, Binding<Set<Object>>> MULTIBINDER_TO_SET = resolverOfBinaryOperator(Utils::union);

	@SuppressWarnings("unchecked")
	public static <T> Function<Set<Binding<Set<T>>>, Binding<Set<T>>> multibinderToSet() {
		return (Function) MULTIBINDER_TO_SET;
	}

	private static class ModuleImpl implements Module {
		private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings;
		private final Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers;

		private ModuleImpl(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings, Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers) {
			this.bindings = bindings;
			this.conflictResolvers = conflictResolvers;
		}

		@Override
		public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindingsMultimap() {
			return bindings;
		}

		@Override
		public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
			return conflictResolvers;
		}
	}
}
