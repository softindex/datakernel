package io.datakernel.di.util;

import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class Utils {
	private Utils() {
		throw new AssertionError("nope.");
	}

	@NotNull
	public static <K, V> Map<K, V> flattenMultimap(Map<K, Set<V>> multimap, Function<K, Function<Set<V>, V>> reducers) {
		return multimap.entrySet().stream()
				.collect(toMap(
						Map.Entry::getKey,
						entry -> {
							Set<V> value = entry.getValue();
							switch (value.size()) {
								case 0:
									throw new IllegalStateException();
								case 1:
									return value.iterator().next();
								default:
									System.out.println("value = " + value);
									return reducers.apply(entry.getKey()).apply(entry.getValue());
							}
						})
				);
	}

	public static <K, V> void combineMultimap(Map<K, Set<V>> accumulator, Map<K, Set<V>> multimap) {
		multimap.forEach((key, set) -> accumulator.computeIfAbsent(key, $ -> new HashSet<>()).addAll(set));
	}

	public static <T> Set<T> union(Set<T> first, Set<T> second) {
		return Stream.concat(first.stream(), second.stream()).collect(toSet());
	}

	public static <T, K, V> Collector<T, ?, Map<K, Set<V>>> toMultimap(Function<? super T, ? extends K> keyMapper,
																	   Function<? super T, ? extends V> valueMapper) {
		return Collectors.toMap(keyMapper, t -> singleton(valueMapper.apply(t)), Utils::union);
	}

	public static void checkArgument(boolean condition) {
		if (!condition) {
			throw new IllegalArgumentException();
		}
	}
}
