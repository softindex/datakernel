package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Dependency;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;
import io.datakernel.util.CollectionUtils;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.combineMultimap;
import static java.util.Arrays.copyOfRange;

public final class Modules {
	private Modules() {
	}

	public static Module combine(Module... modules) {
		return modules.length == 1 ? modules[0] : combine(Arrays.asList(modules));
	}

	public static Module override(Module into, Module replacements) {
		Map<Key<?>, Set<Binding<?>>> bindings = new HashMap<>(into.getBindings());
		bindings.putAll(replacements.getBindings());

		Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>(into.getConflictResolvers());
		conflictResolvers.putAll(replacements.getConflictResolvers());

		Map<Scope, Map<Key<?>, Set<Binding<?>>>> scopeBindings = new HashMap<>();
		replacements.getScopeBindings().forEach((scope, scopeSubBindings) -> scopeBindings.computeIfAbsent(scope, $ -> new HashMap<>()).putAll(scopeSubBindings));

		return new ModuleImpl(bindings, scopeBindings, conflictResolvers);
	}

	public static Module override(Module into, Collection<Module> from) {
		return override(into, combine(from));
	}

	public static Module override(Collection<Module> into, Module from) {
		return override(combine(into), from);
	}

	public static Module override(Collection<Module> into, Collection<Module> from) {
		return override(combine(into), combine(from));
	}

	public static Module combine(Collection<Module> modules) {
		if (modules.size() == 1) {
			return modules.iterator().next();
		}
		Map<Key<?>, Set<Binding<?>>> bindings = new HashMap<>();
		Map<Scope, Map<Key<?>, Set<Binding<?>>>> scopeBindings = new HashMap<>();
		Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>();

		for (Module module : modules) {
			combineMultimap(bindings, module.getBindings());
			module.getScopeBindings().forEach((scope, scopeMultimap) ->
					combineMultimap(scopeBindings.computeIfAbsent(scope, $ -> new HashMap<>()), scopeMultimap));

			module.getConflictResolvers().forEach((k, v) -> conflictResolvers.merge(k, v, ($, $2) -> {
				throw new RuntimeException("more than one conflict resolver per key");
			}));
		}

		return new ModuleImpl(bindings, scopeBindings, conflictResolvers);
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
			List<Binding.Constructor<T>> constructors = new ArrayList<>();
			List<Dependency> keys = new ArrayList<>();
			for (Binding<T> binding : bindings) {
				int offset = keys.size();
				int count = binding.getDependencies().length;
				Collections.addAll(keys, binding.getDependencies());
				constructors.add(args -> binding.getConstructor().construct(copyOfRange(args, offset, count)));
			}
			return new Binding<>(
					bindings.iterator().next().getKey(),
					keys.toArray(new Dependency[0]),
					args -> reducerFunction.apply(constructors.stream().map(constructor -> constructor.construct(args)))
			);
		};
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	public static <T> Function<Set<Binding<T>>, Binding<T>> resolverOfBinaryOperator(BinaryOperator<T> binaryOperator) {
		return resolverOfReducer(stream -> stream.reduce(binaryOperator).get());
	}

	private static final Function<Set<Binding<Set<Object>>>, Binding<Set<Object>>> MULTIBINDER_TO_SET = resolverOfBinaryOperator(CollectionUtils::union);

	@SuppressWarnings("unchecked")
	public static <T> Function<Set<Binding<Set<T>>>, Binding<Set<T>>> multibinderToSet() {
		return (Function) MULTIBINDER_TO_SET;
	}

	private static class ModuleImpl implements Module {
		private final Map<Key<?>, Set<Binding<?>>> bindings;
		private final Map<Scope, Map<Key<?>, Set<Binding<?>>>> scopeBindings;
		private final Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers;

		private ModuleImpl(Map<Key<?>, Set<Binding<?>>> bindings,
						   Map<Scope, Map<Key<?>, Set<Binding<?>>>> scopeBindings,
						   Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers) {
			this.bindings = bindings;
			this.scopeBindings = scopeBindings;
			this.conflictResolvers = conflictResolvers;
		}


		@Override
		public Map<Key<?>, Set<Binding<?>>> getBindings() {
			return bindings;
		}

		@Override
		public Map<Scope, Map<Key<?>, Set<Binding<?>>>> getScopeBindings() {
			return scopeBindings;
		}

		@Override
		public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
			return conflictResolvers;
		}
	}
}
