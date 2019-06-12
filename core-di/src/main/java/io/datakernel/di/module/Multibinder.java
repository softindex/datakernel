package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Dependency;
import io.datakernel.di.util.Constructors;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

@FunctionalInterface
public interface Multibinder<T> {
	Binding<T> multibind(Set<@NotNull Binding<T>> elements);

	static <T> Multibinder<T> ofReducer(Function<Stream<T>, T> reducerFunction) {
		return bindings -> {
			if (bindings.size() == 1) {
				return bindings.iterator().next();
			}
			List<Constructors.Factory<T>> factories = new ArrayList<>();
			List<Dependency> dependencies = new ArrayList<>();
			for (Binding<T> binding : bindings) {
				int from = dependencies.size();
				int to = from + binding.getDependencies().length;
				Collections.addAll(dependencies, binding.getDependencies());
				factories.add(args -> binding.getFactory().create(Arrays.copyOfRange(args, from, to)));
			}
			return Binding.to(
					args -> reducerFunction.apply(factories.stream().map(factory -> factory.create(args))),
					dependencies.toArray(new Dependency[0]));
		};
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	static <T> Multibinder<T> ofBinaryOperator(BinaryOperator<T> binaryOperator) {
		return ofReducer(stream -> stream.reduce(binaryOperator).get());
	}

	Multibinder<Set<Object>> TO_SET = ofReducer(stream -> {
		Set<Object> result = new HashSet<>();
		stream.forEach(result::addAll);
		return result;
	});

	@SuppressWarnings("unchecked")
	static <T> Multibinder<Set<T>> toSet() {
		return (Multibinder) TO_SET;
	}

	Multibinder<Map<Object, Object>> TO_MAP = ofReducer(stream -> {
		Map<Object, Object> result = new HashMap<>();
		stream.forEach(map ->
				map.forEach((k, v) ->
						result.merge(k, v, ($, $2) -> {
							throw new IllegalStateException("Duplicate key " + k);
						})));
		return result;
	});

	@SuppressWarnings("unchecked")
	static <K, V> Multibinder<Map<K, V>> toMap() {
		return (Multibinder) TO_MAP;
	}
}
