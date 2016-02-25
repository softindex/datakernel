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
import io.datakernel.cube.DrillDown;

import java.util.List;
import java.util.Set;

public final class QueryResult {
	private final List records;
	private final Class recordClass;

	private final TotalsPlaceholder totals;
	private final int count;

	private final Set<DrillDown> drillDowns;
	private final Set<List<String>> chains;
	private final List<String> dimensions;
	private final List<String> attributes;
	private final List<String> measures;
	private final List<String> sortedBy;

	private final Object filterAttributesPlaceholder;
	private final List<String> filterAttributes;

	private final Set<String> fields;
	private final Set<String> metadataFields;

	public QueryResult(List records, Class recordClass, TotalsPlaceholder totals, int count, Set<DrillDown> drillDowns,
	                   Set<List<String>> chains, List<String> dimensions, List<String> attributes,
	                   List<String> measures, List<String> sortedBy, Object filterAttributesPlaceholder,
	                   List<String> filterAttributes, Set<String> fields, Set<String> metadataFields) {
		this.records = records;
		this.recordClass = recordClass;
		this.totals = totals;
		this.count = count;
		this.drillDowns = drillDowns;
		this.chains = chains;
		this.dimensions = dimensions;
		this.attributes = attributes;
		this.measures = measures;
		this.sortedBy = sortedBy;
		this.filterAttributesPlaceholder = filterAttributesPlaceholder;
		this.filterAttributes = filterAttributes;
		this.fields = fields;
		this.metadataFields = metadataFields;
	}

	public List getRecords() {
		return records;
	}

	public Class getRecordClass() {
		return recordClass;
	}

	public TotalsPlaceholder getTotals() {
		return totals;
	}

	public int getCount() {
		return count;
	}

	public Set<DrillDown> getDrillDowns() {
		return drillDowns;
	}

	public Set<List<String>> getChains() {
		return chains;
	}

	public List<String> getDimensions() {
		return dimensions;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public List<String> getSortedBy() {
		return sortedBy;
	}

	public Object getFilterAttributesPlaceholder() {
		return filterAttributesPlaceholder;
	}

	public List<String> getFilterAttributes() {
		return filterAttributes;
	}

	public Set<String> getFields() {
		return fields;
	}

	public Set<String> getMetadataFields() {
		return metadataFields;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("records", records)
				.add("recordClass", recordClass)
				.add("totals", totals)
				.add("count", count)
				.add("drillDowns", drillDowns)
				.add("chains", chains)
				.add("dimensions", dimensions)
				.add("attributes", attributes)
				.add("measures", measures)
				.add("sortedBy", sortedBy)
				.add("filterAttributesPlaceholder", filterAttributesPlaceholder)
				.add("filterAttributes", filterAttributes)
				.add("fields", fields)
				.add("metadataFields", metadataFields)
				.toString();
	}
}
