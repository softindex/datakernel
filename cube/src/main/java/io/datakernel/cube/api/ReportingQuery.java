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

import com.google.common.base.MoreObjects;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.cube.CubeQuery;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;

public final class ReportingQuery {
	private List<String> dimensions;
	private List<String> measures;
	private List<String> attributes;
	private AggregationQuery.Predicates filters;
	private List<CubeQuery.Ordering> sort;
	private Integer limit;
	private Integer offset;
	private String searchString;
	private Set<String> fields;
	private Set<String> metadataFields;

	public ReportingQuery() {
	}

	public ReportingQuery(List<String> dimensions, List<String> measures, List<String> attributes,
	                      AggregationQuery.Predicates filters, List<CubeQuery.Ordering> sort,
	                      Integer limit, Integer offset, String searchString, Set<String> fields,
	                      Set<String> metadataFields) {
		this.dimensions = dimensions;
		this.measures = measures;
		this.attributes = attributes;
		this.filters = filters;
		this.sort = sort;
		this.limit = limit;
		this.offset = offset;
		this.searchString = searchString;
		this.fields = fields;
		this.metadataFields = metadataFields;
	}

	public ReportingQuery dimensions(List<String> dimensions) {
		this.dimensions = dimensions;
		return this;
	}

	public ReportingQuery dimensions(String... dimensions) {
		return dimensions(asList(dimensions));
	}

	public List<String> getDimensions() {
		return dimensions;
	}

	public ReportingQuery measures(List<String> measures) {
		this.measures = measures;
		return this;
	}

	public ReportingQuery measures(String... measures) {
		return measures(asList(measures));
	}

	public List<String> getMeasures() {
		return measures;
	}

	public ReportingQuery attributes(List<String> attributes) {
		this.attributes = attributes;
		return this;
	}

	public ReportingQuery attributes(String... attributes) {
		return attributes(asList(attributes));
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public ReportingQuery filters(AggregationQuery.Predicates filters) {
		this.filters = filters;
		return this;
	}

	public AggregationQuery.Predicates getFilters() {
		return filters;
	}

	public ReportingQuery sort(List<CubeQuery.Ordering> sort) {
		this.sort = sort;
		return this;
	}

	public ReportingQuery sort(CubeQuery.Ordering... sort) {
		return sort(asList(sort));
	}

	public List<CubeQuery.Ordering> getSort() {
		return sort;
	}

	public ReportingQuery limit(Integer limit) {
		this.limit = limit;
		return this;
	}

	public Integer getLimit() {
		return limit;
	}

	public ReportingQuery offset(Integer offset) {
		this.offset = offset;
		return this;
	}

	public Integer getOffset() {
		return offset;
	}

	public ReportingQuery search(String searchString) {
		this.searchString = searchString;
		return this;
	}

	public String getSearchString() {
		return searchString;
	}

	public ReportingQuery fields(Set<String> fields) {
		this.fields = fields;
		return this;
	}

	public ReportingQuery fields(String... fields) {
		return fields(newHashSet(fields));
	}

	public Set<String> getFields() {
		return fields;
	}

	public ReportingQuery metadataFields(Set<String> metadataFields) {
		this.metadataFields = metadataFields;
		return this;
	}

	public ReportingQuery metadataFields(String... metadataFields) {
		return metadataFields(newHashSet(metadataFields));
	}

	public Set<String> getMetadataFields() {
		return metadataFields;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("dimensions", dimensions)
				.add("measures", measures)
				.add("attributes", attributes)
				.add("filters", filters)
				.add("sort", sort)
				.add("limit", limit)
				.add("offset", offset)
				.add("searchString", searchString)
				.add("fields", fields)
				.add("metadataFields", metadataFields)
				.toString();
	}
}
