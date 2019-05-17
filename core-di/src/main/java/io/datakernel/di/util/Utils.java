package io.datakernel.di.util;

import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class Utils {
	@NotNull
	public static <K, V> Map<K, V> flattenMultimap(Map<K, Set<V>> multimap, BinaryOperator<V> reducer) {
		return flattenMultimap(multimap, $ -> reducer);
	}

	@NotNull
	public static <K, V> Map<K, V> flattenMultimap(Map<K, Set<V>> multimap, Function<K, BinaryOperator<V>> reducers) {
		return multimap.entrySet()
				.stream()
				.collect(toMap(
						Map.Entry::getKey,
						entry -> entry.getValue()
								.stream()
								.reduce(reducers.apply(entry.getKey()))
								.get()
						)
				);
	}

	public static <K, V> void combineMultimap(Map<K, Set<V>> accumulator, Map<K, Set<V>> multimap) {
		multimap.forEach((key, set) -> accumulator.computeIfAbsent(key, $ -> new HashSet<>()).addAll(set));
	}

}
