package io.datakernel.di.module;

import io.datakernel.di.*;
import io.datakernel.di.util.Utils;

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
		ScopedBindings bindings = ScopedBindings.merge(into.getBindings(), replacements.getBindings());

		Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>(into.getConflictResolvers());
		conflictResolvers.putAll(replacements.getConflictResolvers());
		return new ModuleImpl(bindings, conflictResolvers);
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

		ScopedBindings bindings = ScopedBindings.merge(modules.stream().map(Module::getBindings));
		Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers = new HashMap<>();

		for (Module module : modules) {
			module.getConflictResolvers().forEach((k, v) -> conflictResolvers.merge(k, v, ($, $2) -> {
				throw new RuntimeException("more than one conflict resolver per key");
			}));
		}

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
			List<Binding.Factory<T>> factories = new ArrayList<>();
			List<Dependency> keys = new ArrayList<>();
			for (Binding<T> binding : bindings) {
				int offset = keys.size();
				int count = binding.getDependencies().length;
				Collections.addAll(keys, binding.getDependencies());
				factories.add(args -> binding.getFactory().create(copyOfRange(args, offset, count)));
			}
			return Binding.of(keys.toArray(new Dependency[0]), args -> reducerFunction.apply(factories.stream().map(factory -> factory.create(args))));
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
		private final ScopedBindings bindings;
		private final Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers;

		private ModuleImpl(ScopedBindings bindings, Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> conflictResolvers) {
			this.bindings = bindings;
			this.conflictResolvers = conflictResolvers;
		}

		@Override
		public ScopedBindings getBindings() {
			return bindings;
		}

		@Override
		public Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers() {
			return conflictResolvers;
		}
	}
}
