/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.common;

import io.datakernel.common.collection.CollectionUtils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.datakernel.common.collection.CollectionUtils.set;

public class CollectorsEx {

	private CollectorsEx() {
	}

	public static final Collector<Object, Void, Void> TO_VOID = Collector.of(() -> null, (a, v) -> {}, (a1, a2) -> null, a -> null);

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, Void, Void> toVoid() {
		return (Collector<T, Void, Void>) TO_VOID;
	}

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, T[], T> toFirst() {
		return Collector.of(
				() -> (T[]) new Object[1],
				(a, v) -> { if (a[0] == null) a[0] = v; },
				(a1, a2) -> a1,
				a -> a[0]);
	}

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, T[], T> toLast() {
		return Collector.of(
				() -> (T[]) new Object[1],
				(a, v) -> a[0] = v,
				(a1, a2) -> a1,
				a -> a[0]);
	}

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, List<T>, T[]> toArray(Class<T> type) {
		return Collector.of(
				ArrayList::new,
				List::add,
				(left, right) -> {
					left.addAll(right);
					return left;
				},
				a -> a.toArray((T[]) Array.newInstance(type, a.size())));
	}

	private static final Collector<Boolean, boolean[], Boolean> TO_ALL =
			Collector.of(() -> new boolean[]{true}, (a, t) -> a[0] &= t, (a1, a2) -> {
				a1[0] &= a2[0];
				return a1;
			}, b -> b[0]);

	private static final Collector<Boolean, boolean[], Boolean> TO_ANY =
			Collector.of(() -> new boolean[]{false}, (a, t) -> a[0] |= t, (a1, a2) -> {
				a1[0] |= a2[0];
				return a1;
			}, b -> b[0]);

	private static final Collector<Boolean, boolean[], Boolean> TO_NONE =
			Collector.of(() -> new boolean[]{true}, (a, t) -> a[0] &= !t, (a1, a2) -> {
				a1[0] &= a2[0];
				return a1;
			}, b -> b[0]);

	public static <T> Collector<T, boolean[], Boolean> toAll(Predicate<? super T> predicate) {
		return Collector.of(() -> new boolean[]{true}, (a, t) -> a[0] &= predicate.test(t), (a1, a2) -> {
			a1[0] &= a2[0];
			return a1;
		}, b -> b[0]);
	}

	public static Collector<Boolean, boolean[], Boolean> toAll() {
		return TO_ALL;
	}

	public static <T> Collector<T, boolean[], Boolean> toAny(Predicate<T> predicate) {
		return Collector.of(() -> new boolean[]{false}, (a, t) -> a[0] |= predicate.test(t), (a1, a2) -> {
			a1[0] |= a2[0];
			return a1;
		}, b -> b[0]);
	}

	public static Collector<Boolean, boolean[], Boolean> toAny() {
		return TO_ANY;
	}

	public static <T> Collector<T, boolean[], Boolean> toNone(Predicate<T> predicate) {
		return Collector.of(() -> new boolean[]{true}, (a, t) -> a[0] &= !predicate.test(t), (a1, a2) -> {
			a1[0] &= a2[0];
			return a1;
		}, b -> b[0]);
	}

	public static Collector<Boolean, boolean[], Boolean> toNone() {
		return TO_NONE;
	}

	public static <T> BinaryOperator<T> throwingMerger() {
		return (u, v) -> { throw new IllegalStateException("Duplicate key " + u); };
	}

	public static <K, V> Collector<Entry<K, V>, ?, Map<K, V>> toMap() {
		return Collectors.toMap(Entry::getKey, Entry::getValue);
	}

	public static <T, K, V> Collector<T, ?, Map<K, Set<V>>> toMultimap(Function<? super T, ? extends K> keyMapper,
																	   Function<? super T, ? extends V> valueMapper) {
		return Collectors.toMap(keyMapper, t -> set(valueMapper.apply(t)), CollectionUtils::union);
	}
}
