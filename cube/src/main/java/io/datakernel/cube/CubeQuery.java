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

import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.aggregation.AggregationPredicates;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public final class CubeQuery {
	private List<String> attributes = new ArrayList<>();
	private List<String> measures = new ArrayList<>();
	private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();
	private AggregationPredicate having = AggregationPredicates.alwaysTrue();
	private Integer limit = null;
	private Integer offset = null;
	private List<Ordering> orderings = new ArrayList<>();

	private CubeQuery() {

	}

	public static CubeQuery create() {
		return new CubeQuery();
	}

	// region builders
	public CubeQuery withMeasures(List<String> measures) {
		this.measures = measures;
		return this;
	}

	public CubeQuery withMeasures(String... measures) {
		return withMeasures(asList(measures));
	}

	public CubeQuery withAttributes(List<String> attributes) {
		this.attributes = attributes;
		return this;
	}

	public CubeQuery withAttributes(String... attributes) {
		return withAttributes(asList(attributes));
	}

	public CubeQuery withPredicate(AggregationPredicate predicate) {
		this.predicate = predicate;
		return this;
	}

	public CubeQuery withHaving(AggregationPredicate predicate) {
		this.having = predicate;
		return this;
	}

	public CubeQuery withOrderings(List<CubeQuery.Ordering> orderings) {
		this.orderings = orderings;
		return this;
	}

	public CubeQuery withOrderingAsc(String field) {
		this.orderings.add(CubeQuery.Ordering.asc(field));
		return this;
	}

	public CubeQuery withOrderingDesc(String field) {
		this.orderings.add(CubeQuery.Ordering.desc(field));
		return this;
	}

	public CubeQuery withOrderings(CubeQuery.Ordering... orderings) {
		return withOrderings(asList(orderings));
	}

	public CubeQuery withLimit(Integer limit) {
		this.limit = limit;
		return this;
	}

	public Integer getLimit() {
		return limit;
	}

	public CubeQuery withOffset(Integer offset) {
		this.offset = offset;
		return this;
	}

	// endregion

	// region getters
	public List<String> getAttributes() {
		return attributes;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public AggregationPredicate getPredicate() {
		return predicate;
	}

	public List<CubeQuery.Ordering> getOrderings() {
		return orderings;
	}

	public AggregationPredicate getHaving() {
		return having;
	}

	public Integer getOffset() {
		return offset;
	}

	// endregion

	// region helper classes

	/**
	 * Represents a query result ordering. Contains a propertyName name and ordering (ascending or descending).
	 */
	public static final class Ordering {
		private final String field;
		private final boolean desc;

		private Ordering(String field, boolean desc) {
			this.field = field;
			this.desc = desc;
		}

		public static Ordering asc(String field) {
			return new Ordering(field, false);
		}

		public static Ordering desc(String field) {
			return new Ordering(field, true);
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

			Ordering that = (Ordering) o;

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
	// endregion
}
