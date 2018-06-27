package io.datakernel.util;

import io.datakernel.annotation.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class CollectionUtils {
	private CollectionUtils() {
	}

	public static <D> List<D> concat(Collection<D> list1, Collection<D> list2) {
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

	public static <T> Stream<T> iterate(Supplier<T> supplier, Predicate<T> hasNext) {
		return iterate(supplier.get(), hasNext, $ -> supplier.get());
	}

	public static <T> Stream<T> iterate(T seed, Predicate<T> hasNext, UnaryOperator<T> f) {
		requireNonNull(f);
		return iterate(
				new Iterator<T>() {
					T item = seed;

					@Override
					public boolean hasNext() {
						return hasNext.test(item);
					}

					@Override
					public T next() {
						T next = this.item;
						this.item = f.apply(this.item);
						return next;
					}
				});
	}

	public static <T> Stream<T> iterate(Iterator<T> iterator) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
				Spliterator.ORDERED | Spliterator.IMMUTABLE), false);
	}

	@SuppressWarnings("unchecked")
	public static <T> boolean isShallowEquals(Iterable<? extends T> iterable1, Iterable<? extends T> iterable2) {
		if (iterable1 instanceof Collection && iterable2 instanceof Collection &&
				((Collection<T>) iterable1).size() != ((Collection<T>) iterable2).size()) {
			return false;
		}
		Iterator<? extends T> it1 = iterable1.iterator();
		Iterator<? extends T> it2 = iterable2.iterator();
		while (it1.hasNext() && it2.hasNext()) {
			if (it1.next() != it2.next()) {
				return false;
			}
		}
		assert !it1.hasNext() && !it2.hasNext();
		return true;
	}

	public static <T> String toLimitedString(Collection<T> collection, int limit) {
		return collection.stream()
				.limit(limit)
				.map(Object::toString)
				.collect(joining(",", "[", collection.size() <= limit ? "]" : ", ..and " + (collection.size() - limit) + " more]"));
	}

	public static <T> Iterator<T> asIterator() {
		return new Iterator<T>() {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public T next() {
				throw new NoSuchElementException();
			}
		};
	}

	public static <T> Iterator<T> asIterator(T item) {
		return new Iterator<T>() {
			boolean hasNext = true;

			@Override
			public boolean hasNext() {
				return hasNext;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				hasNext = false;
				return item;
			}
		};
	}

	public static <T> Iterator<T> asIterator(T item1, T item2) {
		return new Iterator<T>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < 2;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				return i++ == 0 ? item1 : item2;
			}
		};
	}

	@SafeVarargs
	public static <T> Iterator<T> asIterator(T... items) {
		return new Iterator<T>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < items.length;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				return items[i++];
			}
		};
	}

	public static <K, V> Map<K, V> keysToMap(Stream<K> stream, Function<K, V> function) {
		LinkedHashMap<K, V> result = new LinkedHashMap<>();
		stream.forEach(key -> result.put(key, function.apply(key)));
		return result;
	}

	public static <K, V> Map<K, V> entriesToMap(Stream<Map.Entry<K, V>> stream) {
		LinkedHashMap<K, V> result = new LinkedHashMap<>();
		stream.forEach(entry -> result.put(entry.getKey(), entry.getValue()));
		return result;
	}

	public static <K, V, T> Map<K, V> transformMapValues(Map<K, T> map, Function<T, V> function) {
		LinkedHashMap<K, V> result = new LinkedHashMap<>(map.size());
		map.forEach((key, value) -> result.put(key, function.apply(value)));
		return result;
	}

	public static int deepHashCode(@Nullable Object value) {
		if (value == null) return 0;
		if (!value.getClass().isArray()) return value.hashCode();
		if (value instanceof Object[]) return Arrays.deepHashCode((Object[]) value);
		if (value instanceof byte[]) return Arrays.hashCode((byte[]) value);
		if (value instanceof short[]) return Arrays.hashCode((short[]) value);
		if (value instanceof int[]) return Arrays.hashCode((int[]) value);
		if (value instanceof long[]) return Arrays.hashCode((long[]) value);
		if (value instanceof float[]) return Arrays.hashCode((float[]) value);
		if (value instanceof double[]) return Arrays.hashCode((double[]) value);
		if (value instanceof boolean[]) return Arrays.hashCode((boolean[]) value);
		if (value instanceof char[]) return Arrays.hashCode((char[]) value);
		throw new AssertionError();
	}
}
