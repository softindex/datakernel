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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.unmodifiableList;

/**
 * Represents a query to aggregation. Contains the list of requested keys, fields, predicates and orderings.
 */
public final class AggregationQuery {
	private List<String> keys = new ArrayList<>();
	private List<String> fields = new ArrayList<>();
	private QueryPredicates predicates = new QueryPredicates();
	private List<QueryOrdering> orderings = new ArrayList<>();

	/**
	 * Represents a query result ordering. Contains a propertyName name and ordering (ascending or descending).
	 */
	public static final class QueryOrdering {
		private final String propertyName;
		private final boolean desc;

		private QueryOrdering(String propertyName, boolean desc) {
			this.propertyName = propertyName;
			this.desc = desc;
		}

		public static QueryOrdering asc(String field) {
			return new QueryOrdering(field, false);
		}

		public static QueryOrdering desc(String field) {
			return new QueryOrdering(field, true);
		}

		public String getPropertyName() {
			return propertyName;
		}

		public boolean isAsc() {
			return !isDesc();
		}

		public boolean isDesc() {
			return desc;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			QueryOrdering that = (QueryOrdering) o;

			if (desc != that.desc) return false;
			if (!propertyName.equals(that.propertyName)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = propertyName.hashCode();
			result = 31 * result + (desc ? 1 : 0);
			return result;
		}

		@Override
		public String toString() {
			return propertyName + " " + (desc ? "desc" : "asc");
		}
	}

	/**
	 * Represents a mapping between key name and {@code QueryPredicate} instance.
	 */
	public static class QueryPredicates {
		private final Map<String, QueryPredicate> map;

		public QueryPredicates() {
			this.map = new LinkedHashMap<>();
		}

		private QueryPredicates(Map<String, QueryPredicate> predicates) {
			this.map = predicates;
		}

		public static QueryPredicates fromMap(Map<String, QueryPredicate> predicates) {
			return new QueryPredicates(predicates);
		}

		public Map<String, QueryPredicate> asUnmodifiableMap() {
			return Collections.unmodifiableMap(map);
		}

		public Map<String, QueryPredicate> asMap() {
			return map;
		}

		public Collection<QueryPredicate> asCollection() {
			return map.values();
		}

		public Set<String> keys() {
			return map.keySet();
		}

		public QueryPredicates add(QueryPredicate predicate) {
			map.put(predicate.key, predicate);
			return this;
		}

		public QueryPredicates eq(String key, Object value) {
			return add(new QueryPredicateEq(key, value));
		}

		public QueryPredicates eq(Map<String, Object> predicates) {
			for (String key : predicates.keySet()) {
				Object value = predicates.get(key);
				add(new QueryPredicateEq(key, value));
			}
			return this;
		}

		public QueryPredicates between(String key, Object from, Object to) {
			return add(new QueryPredicateBetween(key, from, to));
		}

		public QueryPredicates ne(String key, Object value) {
			return add(new QueryPredicateNotEquals(key, value));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			QueryPredicates that = (QueryPredicates) o;

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
	public static abstract class QueryPredicate {
		public final String key;

		protected QueryPredicate(String key) {
			this.key = key;
		}
	}

	/**
	 * Represents an 'equals' query predicate.
	 * Defined by name of key and value,
	 * so that result records should have the value of the specified key equal to given value.
	 */
	public static class QueryPredicateEq extends QueryPredicate {
		public final Object value;

		public QueryPredicateEq(String key, Object value) {
			super(key);
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			QueryPredicateEq that = (QueryPredicateEq) o;

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
	public static class QueryPredicateBetween extends QueryPredicate {
		public final Object from;
		public final Object to;

		public QueryPredicateBetween(String key, Object from, Object to) {
			super(key);
			this.from = from;
			this.to = to;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			QueryPredicateBetween that = (QueryPredicateBetween) o;

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
	public static class QueryPredicateNotEquals extends QueryPredicate {
		public final Object value;

		public QueryPredicateNotEquals(String key, Object value) {
			super(key);
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			QueryPredicateNotEquals that = (QueryPredicateNotEquals) o;
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

	public AggregationQuery(List<String> keys, List<String> fields, QueryPredicates predicates) {
		this.keys.addAll(keys);
		this.fields.addAll(fields);
		this.predicates = predicates;
	}

	public AggregationQuery(List<String> keys, List<String> fields, QueryPredicates predicates,
	                        List<QueryOrdering> orderings) {
		this.keys = keys;
		this.fields = fields;
		this.predicates = predicates;
		this.orderings = orderings;
	}

	public AggregationQuery copyWithDuplicatedPredicatesAndKeys() {
		return new AggregationQuery(newArrayList(this.keys), this.fields,
				QueryPredicates.fromMap(newHashMap(this.predicates.map)), this.orderings);
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

	public List<String> getAllFields() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		result.addAll(this.fields);
		result.addAll(getOrderingFields());
		return new ArrayList<>(result);
	}

	public QueryPredicates getPredicates() {
		return predicates;
	}

	public List<QueryOrdering> getOrderings() {
		return unmodifiableList(orderings);
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

	public AggregationQuery orderAsc(String propertyName) {
		this.orderings.add(QueryOrdering.asc(propertyName));
		return this;
	}

	public AggregationQuery orderDesc(String propertyName) {
		this.orderings.add(QueryOrdering.desc(propertyName));
		return this;
	}

	public AggregationQuery predicates(QueryPredicates predicates) {
		this.predicates = predicates;
		return this;
	}

	public AggregationQuery predicates(List<QueryPredicate> predicates) {
		this.predicates = new QueryPredicates();
		for (QueryPredicate predicate : predicates) {
			this.predicates.add(predicate);
		}
		return this;
	}

	public AggregationQuery addPredicates(List<QueryPredicate> predicates) {
		for (QueryPredicate predicate : predicates) {
			this.predicates.add(predicate);
		}
		return this;
	}

	public AggregationQuery eq(String key, Object value) {
		this.predicates.add(new QueryPredicateEq(key, value));
		return this;
	}

	public AggregationQuery ne(String key, Object value) {
		this.predicates.add(new QueryPredicateNotEquals(key, value));
		return this;
	}

	public AggregationQuery between(String key, Object from, Object to) {
		this.predicates.add(new QueryPredicateBetween(key, from, to));
		return this;
	}

	public Set<String> getOrderingFields() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		for (QueryOrdering ordering : orderings) {
			result.add(ordering.propertyName);
		}
		return result;
	}

	@Override
	public String toString() {
		return "AggregationQuery{" +
				"keys=" + keys +
				", fields=" + fields +
				", predicates=" + predicates +
				", orderings=" + orderings +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AggregationQuery query = (AggregationQuery) o;

		if (!keys.equals(query.keys)) return false;
		if (!fields.equals(query.fields)) return false;
		if (!orderings.equals(query.orderings)) return false;
		if (!predicates.equals(query.predicates)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = keys.hashCode();
		result = 31 * result + fields.hashCode();
		result = 31 * result + predicates.hashCode();
		result = 31 * result + orderings.hashCode();
		return result;
	}
}
