package io.datakernel.di.module;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.DIException;
import io.datakernel.di.core.Dependency;
import io.datakernel.di.core.Key;
import io.datakernel.di.util.Constructors;
import io.datakernel.di.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

@FunctionalInterface
public interface Multibinder<T> {
	Binding<T> multibind(Key<T> key, Set<@NotNull Binding<T>> bindings);

	Multibinder<Object> ERROR_ON_DUPLICATE = (key, bindings) -> {
		throw new DIException(bindings.stream()
				.map(Utils::getLocation)
				.collect(joining("\n\t", "Duplicate bindings for key " + key.getDisplayString() + ":\n\t", "\n")));
	};

	@SuppressWarnings("unchecked")
	static <T> Multibinder<T> getErrorOnDuplicate() {
		return (Multibinder<T>) ERROR_ON_DUPLICATE;
	}

	static <T> Multibinder<T> ofReducer(BiFunction<Key<T>, Stream<T>, T> reducerFunction) {
		return (key, bindings) -> {
			List<Constructors.Factory<T>> factories = new ArrayList<>();
			List<Dependency> dependencies = new ArrayList<>();
			for (Binding<T> binding : bindings) {
				int from = dependencies.size();
				int to = from + binding.getDependencies().length;
				Collections.addAll(dependencies, binding.getDependencies());
				factories.add(args -> binding.getFactory().create(Arrays.copyOfRange(args, from, to)));
			}
			return Binding.to(
					args -> reducerFunction.apply(key, factories.stream().map(factory -> factory.create(args))),
					dependencies.toArray(new Dependency[0]));
		};
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	static <T> Multibinder<T> ofBinaryOperator(BinaryOperator<T> binaryOperator) {
		return ofReducer(($, stream) -> stream.reduce(binaryOperator).get());
	}

	Multibinder<Set<Object>> TO_SET = ofReducer((key, stream) -> {
		Set<Object> result = new HashSet<>();
		stream.forEach(result::addAll);
		return result;
	});

	@SuppressWarnings("unchecked")
	static <T> Multibinder<Set<T>> toSet() {
		return (Multibinder) TO_SET;
	}

	Multibinder<Map<Object, Object>> TO_MAP = ofReducer((key, stream) -> {
		Map<Object, Object> result = new HashMap<>();
		stream.forEach(map ->
				map.forEach((k, v) ->
						result.merge(k, v, ($, $2) -> {
							throw new DIException("Duplicate key " + k + " while merging maps for key " + key.getDisplayString());
						})));
		return result;
	});

	@SuppressWarnings("unchecked")
	static <K, V> Multibinder<Map<K, V>> toMap() {
		return (Multibinder) TO_MAP;
	}

	@SuppressWarnings("unchecked")
	static Multibinder<?> combinedMultibinder(Map<Key<?>, Multibinder<?>> multibinders) {
		return (key, bindings) ->
				((Multibinder<Object>) multibinders.getOrDefault(key, ERROR_ON_DUPLICATE)).multibind(key, bindings);
	}
}
