package io.datakernel.di.module;

import io.datakernel.di.Binding;
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
	private Modules() {}

	public static Module combine(Module... modules) {
		return modules.length == 1 ? modules[0] : combine(Arrays.asList(modules));
	}

	public static Module combine(Collection<Module> modules) {
		if (modules.size() == 1) return modules.iterator().next();
		Map<Key<?>, Set<Binding<?>>> bindings = new HashMap<>();
		Map<Scope, Map<Key<?>, Set<Binding<?>>>> scopeBindings = new HashMap<>();
		Map<Key<?>, Set<BinaryOperator<Binding<?>>>> conflictResolvers = new HashMap<>();

		for (Module module : modules) {
			combineMultimap(bindings, module.getBindings());
			module.getScopeBindings().forEach((scope, scopeMultimap) ->
					combineMultimap(
							scopeBindings.computeIfAbsent(scope, $ -> new HashMap<>()),
							scopeMultimap));
			combineMultimap(conflictResolvers, module.getConflictResolvers());
		}

		return new Module() {
			@Override
			public Map<Key<?>, Set<Binding<?>>> getBindings() {
				return bindings;
			}

			@Override
			public Map<Scope, Map<Key<?>, Set<Binding<?>>>> getScopeBindings() {
				return scopeBindings;
			}

			@Override
			public Map<Key<?>, Set<BinaryOperator<Binding<?>>>> getConflictResolvers() {
				return conflictResolvers;
			}
		};
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
			if (bindings.size() == 1) return bindings.iterator().next();
			List<Binding.Constructor<T>> constructors = new ArrayList<>();
			List<Key<?>> keys = new ArrayList<>();
			for (Binding<T> binding : bindings) {
				int offset = keys.size();
				Collections.addAll(keys, binding.getDependencies());
				int count = binding.getDependencies().length;
				constructors.add(args -> binding.getConstructor().construct(copyOfRange(args, offset, count)));
			}
			return new Binding<>(
					bindings.iterator().next().getKey(),
					keys.toArray(new Key[]{}),
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
}
