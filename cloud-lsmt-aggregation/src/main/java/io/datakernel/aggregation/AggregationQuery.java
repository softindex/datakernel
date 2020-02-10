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

package io.datakernel.aggregation;

import java.util.*;

import static java.util.Collections.unmodifiableList;

/**
 * Represents a query to aggregation. Contains the list of requested keys, fields, predicates and orderings.
 */
public final class AggregationQuery {
	private final List<String> keys = new ArrayList<>();
	private final List<String> measures = new ArrayList<>();
	private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();

	public static AggregationQuery create() {
		return new AggregationQuery();
	}

	public static AggregationQuery create(List<String> keys, List<String> measures) {
		return new AggregationQuery(keys, measures);
	}

	public static AggregationQuery create(List<String> keys, List<String> measures, AggregationPredicate predicate) {
		return new AggregationQuery(keys, measures, predicate);
	}

	private AggregationQuery() {
	}

	private AggregationQuery(List<String> keys, List<String> measures) {
		this.keys.addAll(keys);
		this.measures.addAll(measures);
	}

	private AggregationQuery(List<String> keys, List<String> measures, AggregationPredicate predicate) {
		this.keys.addAll(keys);
		this.measures.addAll(measures);
		this.predicate = predicate;
	}

	public List<String> getKeys() {
		return keys;
	}

	public List<String> getMeasures() {
		return unmodifiableList(measures);
	}

	public Set<String> getAllKeys() {
		LinkedHashSet<String> result = new LinkedHashSet<>();
		result.addAll(keys);
		result.addAll(predicate.getDimensions());
		return result;
	}

	public AggregationPredicate getPredicate() {
		return predicate;
	}

	public AggregationQuery withKey(String key) {
		this.keys.add(key);
		return this;
	}

	public AggregationQuery withKeys(List<String> keys) {
		this.keys.addAll(keys);
		return this;
	}

	public AggregationQuery withKeys(String... keys) {
		this.keys.addAll(Arrays.asList(keys));
		return this;
	}

	public AggregationQuery withMeasures(List<String> fields) {
		this.measures.addAll(fields);
		return this;
	}

	public AggregationQuery withMeasures(String... fields) {
		this.measures.addAll(Arrays.asList(fields));
		return this;
	}

	public AggregationQuery withMeasure(String field) {
		this.measures.add(field);
		return this;
	}

	public AggregationQuery withPredicate(AggregationPredicate predicate) {
		this.predicate = predicate;
		return this;
	}

	public AggregationQuery withPredicates(List<AggregationPredicate> predicates) {
		this.predicate = AggregationPredicates.and(predicates);
		return this;
	}

//	public AggregationQuery addPredicates(List<Predicate> predicates) {
//		for (Predicate predicate : predicates) {
//			this.predicates.add(predicate);
//		}
//		return this;
//	}

	@Override
	public String toString() {
		return "AggregationQuery{" +
				"keys=" + keys +
				", fields=" + measures +
				", predicate=" + predicate +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AggregationQuery query = (AggregationQuery) o;
		return keys.equals(query.keys) &&
				measures.equals(query.measures) &&
				predicate.equals(query.predicate);
	}

	@Override
	public int hashCode() {
		int result = keys.hashCode();
		result = 31 * result + measures.hashCode();
		result = 31 * result + predicate.hashCode();
		return result;
	}
}
