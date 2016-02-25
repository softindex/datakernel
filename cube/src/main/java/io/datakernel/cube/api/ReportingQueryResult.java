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
import java.util.Map;
import java.util.Set;

public final class ReportingQueryResult {
	private final List<Map<String, Object>> records;
	private final Map<String, Object> totals;
	private final int count;

	private final Set<DrillDown> drillDowns;
	private final List<String> dimensions;
	private final List<String> attributes;
	private final List<String> measures;
	private final Map<String, Object> filterAttributes;
	private final List<String> sortedBy;

	public ReportingQueryResult(List<Map<String, Object>> records, Map<String, Object> totals, int count,
	                            Set<DrillDown> drillDowns, List<String> dimensions, List<String> attributes,
	                            List<String> measures, Map<String, Object> filterAttributes,
	                            List<String> sortedBy) {
		this.records = records;
		this.totals = totals;
		this.count = count;
		this.drillDowns = drillDowns;
		this.dimensions = dimensions;
		this.attributes = attributes;
		this.measures = measures;
		this.filterAttributes = filterAttributes;
		this.sortedBy = sortedBy;
	}

	public List<Map<String, Object>> getRecords() {
		return records;
	}

	public Map<String, Object> getTotals() {
		return totals;
	}

	public int getCount() {
		return count;
	}

	public Set<DrillDown> getDrillDowns() {
		return drillDowns;
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

	public Map<String, Object> getFilterAttributes() {
		return filterAttributes;
	}

	public List<String> getSortedBy() {
		return sortedBy;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("records", records)
				.add("totals", totals)
				.add("count", count)
				.add("drillDowns", drillDowns)
				.add("dimensions", dimensions)
				.add("attributes", attributes)
				.add("measures", measures)
				.add("filterAttributes", filterAttributes)
				.add("sortedBy", sortedBy)
				.toString();
	}
}
