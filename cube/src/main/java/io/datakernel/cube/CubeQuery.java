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

import io.datakernel.aggregation_db.AggregationQuery;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;

public final class CubeQuery {
	private AggregationQuery aggregationQuery = AggregationQuery.create();
	private List<Ordering> orderings = new ArrayList<>();

	public static CubeQuery create() {return new CubeQuery();}

	public static CubeQuery create(List<String> dimensions, List<String> measures, AggregationQuery.Predicates predicates,
	                               List<Ordering> orderings) {return new CubeQuery(dimensions, measures, predicates, orderings);}

	public static CubeQuery create(List<String> dimensions, List<String> measures) {return new CubeQuery(dimensions, measures);}

	/**
	 * Represents a query result ordering. Contains a propertyName name and ordering (ascending or descending).
	 */
	public static final class Ordering {
		private final String propertyName;
		private final boolean desc;

		private Ordering(String propertyName, boolean desc) {
			this.propertyName = propertyName;
			this.desc = desc;
		}

		public static Ordering asc(String field) {
			return new Ordering(field, false);
		}

		public static Ordering desc(String field) {
			return new Ordering(field, true);
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

			Ordering that = (Ordering) o;

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

	private CubeQuery() {
	}

	private CubeQuery(List<String> dimensions, List<String> measures, AggregationQuery.Predicates predicates,
	                  List<Ordering> orderings) {
		this.aggregationQuery = AggregationQuery.create(dimensions, measures, predicates);
		this.orderings = orderings;
	}

	private CubeQuery(List<String> dimensions, List<String> measures) {
		this.aggregationQuery = AggregationQuery.create(dimensions, measures);
	}

	public AggregationQuery.Predicates getPredicates() {
		return aggregationQuery.getPredicates();
	}

	public CubeQuery withDimension(String dimension) {
		aggregationQuery.withKey(dimension);
		return this;
	}

	public CubeQuery withDimensions(List<String> dimensions) {
		aggregationQuery.withKeys(dimensions);
		return this;
	}

	public CubeQuery withDimensions(String... dimensions) {
		aggregationQuery.withKeys(dimensions);
		return this;
	}

	public CubeQuery withMeasures(List<String> fields) {
		aggregationQuery.withFields(fields);
		return this;
	}

	public CubeQuery withMeasures(String... fields) {
		aggregationQuery.withFields(fields);
		return this;
	}

	public CubeQuery withField(String field) {
		aggregationQuery.withField(field);
		return this;
	}

	public CubeQuery withOrdering(Ordering ordering) {
		this.orderings.add(ordering);
		return this;
	}

	public CubeQuery withOrderAsc(String propertyName) {
		this.orderings.add(Ordering.asc(propertyName));
		return this;
	}

	public CubeQuery withOrderDesc(String propertyName) {
		this.orderings.add(Ordering.desc(propertyName));
		return this;
	}

	public CubeQuery withPredicates(AggregationQuery.Predicates predicates) {
		aggregationQuery.withPredicates(predicates);
		return this;
	}

	public CubeQuery withPredicates(List<AggregationQuery.Predicate> predicates) {
		aggregationQuery.withPredicates(predicates);
		return this;
	}

	public CubeQuery withEq(String dimension, Object value) {
		aggregationQuery.withEq(dimension, value);
		return this;
	}

	public CubeQuery withNe(String dimension, Object value) {
		aggregationQuery.withNe(dimension, value);
		return this;
	}

	public CubeQuery withBetween(String dimension, Object from, Object to) {
		aggregationQuery.withBetween(dimension, from, to);
		return this;
	}

//	public CubeQuery addPredicates(List<AggregationQuery.Predicate> predicates) {
//		aggregationQuery.addPredicates(predicates);
//		return this;
//	}

	public List<String> getResultDimensions() {
		return aggregationQuery.getResultKeys();
	}

	public List<String> getResultMeasures() {
		return aggregationQuery.getResultFields();
	}

	public List<Ordering> getOrderings() {
		return unmodifiableList(orderings);
	}

	public Set<String> getOrderingFields() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		for (Ordering ordering : orderings) {
			result.add(ordering.propertyName);
		}
		return result;
	}

	public List<String> getAllKeys() {
		return aggregationQuery.getAllKeys();
	}

	public AggregationQuery getAggregationQuery() {
		return aggregationQuery;
	}

	@Override
	public String toString() {
		return "CubeQuery{" +
				"keys=" + aggregationQuery.getResultKeys() +
				", fields=" + aggregationQuery.getResultFields() +
				", predicates=" + aggregationQuery.getPredicates() +
				", orderings=" + orderings +
				'}';
	}
}
