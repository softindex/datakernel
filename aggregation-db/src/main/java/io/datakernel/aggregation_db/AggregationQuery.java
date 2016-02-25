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

package io.datakernel.aggregation_db;

import java.util.*;

import static java.util.Collections.unmodifiableList;

/**
 * Represents a query to aggregation. Contains the list of requested keys, fields, predicates and orderings.
 */
public final class AggregationQuery {
	private List<String> keys = new ArrayList<>();
	private List<String> fields = new ArrayList<>();
	private Predicates predicates = new Predicates();

	/**
	 * Represents a mapping between key name and {@code QueryPredicate} instance.
	 */
	public static class Predicates {
		private final Map<String, Predicate> map;

		public Predicates() {
			this.map = new LinkedHashMap<>();
		}

		private Predicates(Map<String, Predicate> predicates) {
			this.map = predicates;
		}

		public static Predicates fromMap(Map<String, Predicate> predicates) {
			return new Predicates(predicates);
		}

		public Map<String, Predicate> asUnmodifiableMap() {
			return Collections.unmodifiableMap(map);
		}

		public Map<String, Predicate> asMap() {
			return map;
		}

		public Collection<Predicate> asCollection() {
			return map.values();
		}

		public Set<String> keys() {
			return map.keySet();
		}

		public Predicates add(Predicate predicate) {
			map.put(predicate.key, predicate);
			return this;
		}

		public Predicates eq(String key, Object value) {
			return add(new PredicateEq(key, value));
		}

		public Predicates eq(Map<String, Object> predicates) {
			for (String key : predicates.keySet()) {
				Object value = predicates.get(key);
				add(new PredicateEq(key, value));
			}
			return this;
		}

		public Predicates between(String key, Object from, Object to) {
			return add(new PredicateBetween(key, from, to));
		}

		public Predicates ne(String key, Object value) {
			return add(new PredicateNotEquals(key, value));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Predicates that = (Predicates) o;

			if (!map.equals(that.map)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return map.hashCode();
		}

		@Override
		public String toString() {
			return map.toString();
		}
	}

	/**
	 * Represents a query predicate.
	 */
	public static abstract class Predicate {
		public final String key;

		protected Predicate(String key) {
			this.key = key;
		}
	}

	/**
	 * Represents an 'equals' query predicate.
	 * Defined by name of key and value,
	 * so that result records should have the value of the specified key equal to given value.
	 */
	public static class PredicateEq extends Predicate {
		public final Object value;

		public PredicateEq(String key, Object value) {
			super(key);
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateEq that = (PredicateEq) o;

			if (!value.equals(that.value)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return value.hashCode();
		}

		@Override
		public String toString() {
			return key + "=" + value;
		}
	}

	/**
	 * Represents a 'between' query predicate.
	 * Defined by name of key and two values: 'from' and 'to',
	 * so that result records should have the value of the specified key in range [from; to] (range is inclusive!).
	 */
	public static class PredicateBetween extends Predicate {
		public final Object from;
		public final Object to;

		public PredicateBetween(String key, Object from, Object to) {
			super(key);
			this.from = from;
			this.to = to;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			PredicateBetween that = (PredicateBetween) o;

			if (!from.equals(that.from)) return false;
			if (!to.equals(that.to)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = from.hashCode();
			result = 31 * result + to.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return key + " BETWEEN " + from + " AND " + to;
		}
	}

	/**
	 * Represents a 'not equals' query predicate.
	 * Defined by name of key and value, so that result records should have the value of the specified key not equal to given value.
	 * Implemented through post-filtering of records read from physical storage.
	 */
	public static class PredicateNotEquals extends Predicate {
		public final Object value;

		public PredicateNotEquals(String key, Object value) {
			super(key);
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			PredicateNotEquals that = (PredicateNotEquals) o;
			return Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(value);
		}

		@Override
		public String toString() {
			return key + "!=" + value;
		}
	}

	public AggregationQuery() {
	}

	public AggregationQuery(List<String> keys, List<String> fields) {
		this.keys.addAll(keys);
		this.fields.addAll(fields);
	}

	public AggregationQuery(List<String> keys, List<String> fields, Predicates predicates) {
		this.keys.addAll(keys);
		this.fields.addAll(fields);
		this.predicates = predicates;
	}

	public List<String> getResultKeys() {
		return keys;
	}

	public List<String> getResultFields() {
		return unmodifiableList(fields);
	}

	public List<String> getAllKeys() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		result.addAll(keys);
		result.addAll(predicates.keys());
		return new ArrayList<>(result);
	}

	public Predicates getPredicates() {
		return predicates;
	}

	public AggregationQuery key(String key) {
		this.keys.add(key);
		return this;
	}

	public AggregationQuery keys(List<String> keys) {
		this.keys.addAll(keys);
		return this;
	}

	public AggregationQuery keys(String... keys) {
		this.keys.addAll(Arrays.asList(keys));
		return this;
	}

	public AggregationQuery fields(List<String> fields) {
		this.fields.addAll(fields);
		return this;
	}

	public AggregationQuery fields(String... fields) {
		this.fields.addAll(Arrays.asList(fields));
		return this;
	}

	public AggregationQuery field(String field) {
		this.fields.add(field);
		return this;
	}

	public AggregationQuery predicates(Predicates predicates) {
		this.predicates = predicates;
		return this;
	}

	public AggregationQuery predicates(List<Predicate> predicates) {
		this.predicates = new Predicates();
		for (Predicate predicate : predicates) {
			this.predicates.add(predicate);
		}
		return this;
	}

	public AggregationQuery addPredicates(List<Predicate> predicates) {
		for (Predicate predicate : predicates) {
			this.predicates.add(predicate);
		}
		return this;
	}

	public AggregationQuery eq(String key, Object value) {
		this.predicates.add(new PredicateEq(key, value));
		return this;
	}

	public AggregationQuery ne(String key, Object value) {
		this.predicates.add(new PredicateNotEquals(key, value));
		return this;
	}

	public AggregationQuery between(String key, Object from, Object to) {
		this.predicates.add(new PredicateBetween(key, from, to));
		return this;
	}

	@Override
	public String toString() {
		return "AggregationQuery{" +
				"keys=" + keys +
				", fields=" + fields +
				", predicates=" + predicates +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AggregationQuery query = (AggregationQuery) o;

		if (!keys.equals(query.keys)) return false;
		if (!fields.equals(query.fields)) return false;
		if (!predicates.equals(query.predicates)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = keys.hashCode();
		result = 31 * result + fields.hashCode();
		result = 31 * result + predicates.hashCode();
		return result;
	}
}
