/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.cube;

import java.util.*;

import static java.util.Collections.unmodifiableSet;
import static java.util.Collections.unmodifiableSortedMap;

public final class RangeTree<K, V> {

	public static final class Segment<V> {
		private final LinkedHashSet<V> set = new LinkedHashSet<>();
		private final LinkedHashSet<V> closing = new LinkedHashSet<>();

		public Set<V> getSet() {
			return unmodifiableSet(set);
		}

		public Set<V> getClosingSet() {
			return unmodifiableSet(closing);
		}

		private static <V> Segment<V> cloneOf(Segment<V> segment) {
			Segment<V> result = new Segment<>();
			result.set.addAll(segment.set);
			result.closing.addAll(segment.closing);
			return result;
		}

		@Override
		public String toString() {
			return set + "; " + closing;
		}
	}

	private final TreeMap<K, Segment<V>> segments;

	public SortedMap<K, Segment<V>> getSegments() {
		return unmodifiableSortedMap(segments);
	}

	private RangeTree(TreeMap<K, Segment<V>> segments) {
		this.segments = segments;
	}

	public RangeTree() {
		this(new TreeMap<K, Segment<V>>());
	}

	public static <K, V> RangeTree<K, V> cloneOf(RangeTree<K, V> source) {
		RangeTree<K, V> result = new RangeTree<>();
		result.segments.putAll(source.segments);
		for (Map.Entry<K, Segment<V>> entry : result.segments.entrySet()) {
			entry.setValue(Segment.cloneOf(entry.getValue()));
		}
		return result;
	}

	public void put(K lower, K upper, V value) {
		if (!segments.containsKey(lower)) {
			Map.Entry<K, Segment<V>> floorEntry = segments.floorEntry(lower);
			Segment<V> newEntry = new Segment<>();
			if (floorEntry != null)
				newEntry.set.addAll(floorEntry.getValue().set);
			segments.put(lower, newEntry);
		}

		if (!segments.containsKey(upper)) {
			Map.Entry<K, Segment<V>> floorEntry = segments.floorEntry(upper);
			Segment<V> newSegment = new Segment<>();
			if (floorEntry != null)
				newSegment.set.addAll(floorEntry.getValue().set);
			segments.put(upper, newSegment);
			newSegment.closing.add(value);
		} else {
			Segment<V> segment = segments.get(upper);
			segment.closing.add(value);
		}

		SortedMap<K, Segment<V>> subMap = segments.subMap(lower, upper);
		for (Map.Entry<K, Segment<V>> entry : subMap.entrySet()) {
			entry.getValue().set.add(value);
		}
	}

	public boolean remove(K lower, K upper, V value) {
		boolean removed = false;
		Segment<V> upperSegment = segments.get(upper);
		upperSegment.closing.remove(value);
		if (upperSegment.set.isEmpty() && upperSegment.closing.isEmpty()) {
			removed = segments.remove(upper) != null;
		}

		Iterator<Segment<V>> it = segments.subMap(lower, upper).values().iterator();
		while (it.hasNext()) {
			Segment<V> segment = it.next();
			removed |= segment.set.remove(value);
			if (segment.set.isEmpty() && segment.closing.isEmpty()) {
				it.remove();
			}
		}
		return removed;
	}

	public Set<V> get(K key) {
		LinkedHashSet<V> result = new LinkedHashSet<>();
		for (Map.Entry<K, Segment<V>> entry : segments.headMap(key, true).descendingMap().entrySet()) {
			result.addAll(entry.getValue().set);
			if (!entry.getKey().equals(key)) {
				break;
			}
			result.addAll(entry.getValue().closing);
		}
		return result;
	}

	public Set<V> getRange(K lower, K upper) {
		LinkedHashSet<V> result = new LinkedHashSet<>();

		Map.Entry<K, Segment<V>> floorEntry = segments.floorEntry(lower);
		if (floorEntry != null) {
			result.addAll(floorEntry.getValue().set);
		}

		SortedMap<K, Segment<V>> subMap = segments.subMap(lower, upper);
		for (Map.Entry<K, Segment<V>> entry : subMap.entrySet()) {
			result.addAll(entry.getValue().set);
			result.addAll(entry.getValue().closing);
		}

		Segment<V> upperEntry = segments.get(upper);
		if (upperEntry != null) {
			result.addAll(upperEntry.set);
			result.addAll(upperEntry.closing);
		}

		return result;
	}

	public Set<V> getAll() {
		LinkedHashSet<V> result = new LinkedHashSet<>();
		for (Segment<V> segment : segments.values()) {
			result.addAll(segment.closing);
		}
		return result;
	}

}
