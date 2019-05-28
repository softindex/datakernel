package io.datakernel.di.util;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.*;

public final class Utils {
	private Utils() {
		throw new AssertionError("nope.");
	}

	private static final BiConsumer<Map<Object, Set<Object>>, Map<Object, Set<Object>>> MULTIMAP_MERGER =
			(into, from) -> from.forEach((k, v) -> into.computeIfAbsent(k, $ -> new HashSet<>()).addAll(v));

	@SuppressWarnings("unchecked")
	public static <K, V> BiConsumer<Map<K, Set<V>>, Map<K, Set<V>>> multimapMerger() {
		return (BiConsumer<Map<K, Set<V>>, Map<K, Set<V>>>) (BiConsumer) MULTIMAP_MERGER;
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
								default: {
									Function<Set<V>, V> conflictResolver = reducers.apply(entry.getKey());
									if (conflictResolver == null) {
										throw new IllegalStateException("Duplicate bindings for " + entry.getKey() + "\n" +
												entry.getValue().stream().map(Object::toString).map(s -> "\t\t" + s).collect(joining("\n")));
									}
									return conflictResolver.apply(entry.getValue());
								}
							}
						})
				);
	}

	public static void mergeConflictResolvers(Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> into, Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> from) {
		from.forEach((k, v) -> into.merge(k, v, (oldResolver, newResolver) -> {
			if (!oldResolver.equals(newResolver)) {
				throw new RuntimeException("more than one conflict resolver per key");
			}
			return oldResolver;
		}));
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
