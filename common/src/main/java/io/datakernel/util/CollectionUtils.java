package io.datakernel.util;

import io.datakernel.annotation.Nullable;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class CollectionUtils {
	private CollectionUtils() {
	}

	public static <D> List<D> concat(List<D> list1, List<D> list2) {
		List<D> result = new ArrayList<>(list1.size() + list2.size());
		result.addAll(list1);
		result.addAll(list2);
		return result;
	}

	@SafeVarargs
	public static <T> Set<T> set(T... items) {
		return new LinkedHashSet<>(asList(items));
	}

	public static <T> Set<T> difference(Set<? extends T> a, Set<? extends T> b) {
		return a.stream().filter(t -> !b.contains(t)).collect(toSet());
	}

	public static <T> Set<T> intersection(Set<? extends T> a, Set<? extends T> b) {
		return a.size() < b.size() ?
				a.stream().filter(b::contains).collect(toSet()) :
				b.stream().filter(a::contains).collect(toSet());
	}

	public static <T> boolean hasIntersection(Set<? extends T> a, Set<? extends T> b) {
		return a.size() < b.size() ?
				a.stream().anyMatch(b::contains) :
				b.stream().anyMatch(a::contains);
	}

	public static <T> Set<T> union(Set<? extends T> a, Set<? extends T> b) {
		return Stream.concat(a.stream(), b.stream()).collect(toSet());
	}

	public static <T> T first(Iterable<T> iterable) {
		return iterable.iterator().next();
	}

	public static <T> Set<T> nullToEmpty(@Nullable Set<T> set) {
		return set != null ? set : emptySet();
	}

	public static <T> List<T> nullToEmpty(@Nullable List<T> list) {
		return list != null ? list : emptyList();
	}

	public static <K, V> Map<K, V> nullToEmpty(@Nullable Map<K, V> map) {
		return map != null ? map : emptyMap();
	}

	public static <T> Collection<T> nullToEmpty(@Nullable Collection<T> collection) {
		return collection != null ? collection : emptyList();
	}

	public static <T> Iterable<T> nullToEmpty(@Nullable Iterable<T> iterable) {
		return iterable != null ? iterable : emptyList();
	}

	public static <T> Iterator<T> nullToEmpty(@Nullable Iterator<T> iterator) {
		return iterator != null ? iterator : emptyIterator();
	}

	public static <T> List<T> list() {
		return emptyList();
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> list(T... items) {
		return asList(items);
	}

	public static <T> String toLimitedString(Collection<T> collection, int limit) {
		return collection.stream()
				.limit(limit)
				.map(Object::toString)
				.collect(joining(",", "[", collection.size() <= limit ? "]" : ",..]"));
	}

}
