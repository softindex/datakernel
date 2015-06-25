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

import static java.util.Collections.unmodifiableList;

/**
 * Represents a query to cube. Contains the list of requested dimensions, measures, predicates and orderings.
 */
public final class CubeQuery {
	private final List<String> dimensions = new ArrayList<>();
	private final List<String> measures = new ArrayList<>();
	private CubePredicates predicates = new CubePredicates();
	private final List<CubeOrdering> orderings = new ArrayList<>();

	/**
	 * Represents a query result ordering. Contains a field name and ordering (ascending or descending).
	 */
	public static final class CubeOrdering {
		private final String field;
		private final boolean desc;

		private CubeOrdering(String field, boolean desc) {
			this.field = field;
			this.desc = desc;
		}

		public static CubeOrdering asc(String field) {
			return new CubeOrdering(field, false);
		}

		public static CubeOrdering desc(String field) {
			return new CubeOrdering(field, true);
		}

		public String getField() {
			return field;
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

			CubeOrdering that = (CubeOrdering) o;

			if (desc != that.desc) return false;
			if (!field.equals(that.field)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = field.hashCode();
			result = 31 * result + (desc ? 1 : 0);
			return result;
		}

		@Override
		public String toString() {
			return field + " " + (desc ? "desc" : "asc");
		}
	}

	/**
	 * Represents a mapping between dimension name and {@code CubePredicate} instance.
	 */
	public static class CubePredicates {
		private final Map<String, CubePredicate> map = new LinkedHashMap<>();

		public Map<String, CubePredicate> map() {
			return Collections.unmodifiableMap(map);
		}

		public Collection<CubePredicate> predicates() {
			return map.values();
		}

		public Set<String> dimensions() {
			return map.keySet();
		}

		public CubePredicates add(CubePredicate predicate) {
			map.put(predicate.dimension, predicate);
			return this;
		}

		public CubePredicates eq(String dimension, Object value) {
			return add(new CubePredicateEq(dimension, value));
		}

		public CubePredicates eq(Map<String, Object> predicates) {
			for (String dimension : predicates.keySet()) {
				Object value = predicates.get(dimension);
				add(new CubePredicateEq(dimension, value));
			}
			return this;
		}

		public CubePredicates between(String dimension, Object from, Object to) {
			return add(new CubePredicateBetween(dimension, from, to));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			CubePredicates that = (CubePredicates) o;

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
	 * Represents a cube query predicate.
	 */
	public static abstract class CubePredicate {
		public final String dimension;

		protected CubePredicate(String dimension) {
			this.dimension = dimension;
		}
	}

	/**
	 * Represents an 'equals' cube query predicate.
	 * Defined by name of dimension and value,
	 * so that result records should have the value of the specified dimension equal to given value.
	 */
	public static class CubePredicateEq extends CubePredicate {
		public final Object value;

		protected CubePredicateEq(String dimension, Object value) {
			super(dimension);
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			CubePredicateEq that = (CubePredicateEq) o;

			if (!value.equals(that.value)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return value.hashCode();
		}

		@Override
		public String toString() {
			return dimension + "=" + value;
		}
	}

	/**
	 * Represents a 'between' cube query predicate.
	 * Defined by name of dimension and two values: 'from' and 'to',
	 * so that result records should have the value of the specified dimension in range [from; to] (range is inclusive!).
	 */
	public static class CubePredicateBetween extends CubePredicate {
		public final Object from;
		public final Object to;

		protected CubePredicateBetween(String dimension, Object from, Object to) {
			super(dimension);
			this.from = from;
			this.to = to;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			CubePredicateBetween that = (CubePredicateBetween) o;

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
			return dimension + " BETWEEN " + from + " AND " + to;
		}
	}

	public CubeQuery() {
	}

	public CubeQuery(List<String> dimensions, List<String> measures) {
		this.dimensions.addAll(dimensions);
		this.measures.addAll(measures);
	}

	public CubeQuery(List<String> dimensions, List<String> measures, CubePredicates predicates) {
		this.dimensions.addAll(dimensions);
		this.measures.addAll(measures);
		this.predicates = predicates;
	}

	public CubeQuery(List<String> dimensions, List<String> measures,
	                 Map<String, Object> predicates) {
		this.dimensions.addAll(dimensions);
		this.measures.addAll(measures);
		this.predicates.eq(predicates);
	}

	public List<String> getResultDimensions() {
		return dimensions;
	}

	public List<String> getResultMeasures() {
		return unmodifiableList(measures);
	}

	public List<String> getAllDimensions() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		result.addAll(dimensions);
		result.addAll(predicates.dimensions());
		return new ArrayList<>(result);
	}

	public List<String> getAllMeasures() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		result.addAll(this.measures);
		result.addAll(getOrderingFields());
		return new ArrayList<>(result);
	}

	public CubePredicates getPredicates() {
		return predicates;
	}

	public List<CubeOrdering> getOrderings() {
		return unmodifiableList(orderings);
	}

	public CubeQuery dimension(String dimension) {
		this.dimensions.add(dimension);
		return this;
	}

	public CubeQuery dimensions(List<String> dimensions) {
		this.dimensions.addAll(dimensions);
		return this;
	}

	public CubeQuery dimensions(String... dimensions) {
		this.dimensions.addAll(Arrays.asList(dimensions));
		return this;
	}

	public CubeQuery measures(List<String> measures) {
		this.measures.addAll(measures);
		return this;
	}

	public CubeQuery measures(String... measures) {
		this.measures.addAll(Arrays.asList(measures));
		return this;
	}

	public CubeQuery measure(String measure) {
		this.measures.add(measure);
		return this;
	}

	public CubeQuery orderAsc(String measure) {
		this.orderings.add(CubeOrdering.asc(measure));
		return this;
	}

	public CubeQuery orderDesc(String measure) {
		this.orderings.add(CubeOrdering.desc(measure));
		return this;
	}

	public CubeQuery predicates(CubePredicates predicates) {
		this.predicates = predicates;
		return this;
	}

	public CubeQuery eq(String dimension, Object value) {
		this.predicates.add(new CubePredicateEq(dimension, value));
		return this;
	}

	public CubeQuery between(String dimension, Object from, Object to) {
		this.predicates.add(new CubePredicateBetween(dimension, from, to));
		return this;
	}

	public Set<String> getOrderingFields() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		for (CubeOrdering ordering : orderings) {
			result.add(ordering.field);
		}
		return result;
	}

	@Override
	public String toString() {
		return "CubeQuery{" +
				"dimensions=" + dimensions +
				", measures=" + measures +
				", predicates=" + predicates +
				", orderings=" + orderings +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CubeQuery query = (CubeQuery) o;

		if (!dimensions.equals(query.dimensions)) return false;
		if (!measures.equals(query.measures)) return false;
		if (!orderings.equals(query.orderings)) return false;
		if (!predicates.equals(query.predicates)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = dimensions.hashCode();
		result = 31 * result + measures.hashCode();
		result = 31 * result + predicates.hashCode();
		result = 31 * result + orderings.hashCode();
		return result;
	}
}
