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

package io.datakernel.cube.api;

import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.cube.CubeQuery;

import java.util.List;

import static java.util.Arrays.asList;

public final class CubeHttpQuery {
	private List<String> dimensions;
	private List<String> measures;
	private List<String> attributes;
	private AggregationQuery.Predicates filters;
	private CubeQuery.Ordering sort;
	private Integer limit;
	private Integer offset;

	private CubeHttpQuery() {}

	public static CubeHttpQuery createCubeHttpQuery() {return new CubeHttpQuery();}

	public CubeHttpQuery withDimensions(List<String> dimensions) {
		this.dimensions = dimensions;
		return this;
	}

	public CubeHttpQuery withDimensions(String... dimensions) {
		return withDimensions(asList(dimensions));
	}

	public List<String> getDimensions() {
		return dimensions;
	}

	public CubeHttpQuery withMeasures(List<String> measures) {
		this.measures = measures;
		return this;
	}

	public CubeHttpQuery withMeasures(String... measures) {
		return withMeasures(asList(measures));
	}

	public List<String> getMeasures() {
		return measures;
	}

	public CubeHttpQuery withAttributes(List<String> attributes) {
		this.attributes = attributes;
		return this;
	}

	public CubeHttpQuery withAttributes(String... attributes) {
		return withMeasures(asList(attributes));
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public CubeHttpQuery withFilters(AggregationQuery.Predicates filters) {
		this.filters = filters;
		return this;
	}

	public AggregationQuery.Predicates getFilters() {
		return filters;
	}

	public CubeHttpQuery withSort(CubeQuery.Ordering sort) {
		this.sort = sort;
		return this;
	}

	public CubeQuery.Ordering getSort() {
		return sort;
	}

	public CubeHttpQuery withLimit(Integer limit) {
		this.limit = limit;
		return this;
	}

	public Integer getLimit() {
		return limit;
	}

	public CubeHttpQuery withOffset(Integer offset) {
		this.offset = offset;
		return this;
	}

	public Integer getOffset() {
		return offset;
	}
}
