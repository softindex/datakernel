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

import java.util.*;

public final class QueryResult {
	private final RecordScheme recordScheme;
	private final List<String> attributes;
	private final List<String> measures;
	private final List<String> sortedBy;

	private final List<Record> records;
	private final Record totals;
	private final int totalCount;

	private final Collection<Drilldown> drilldowns;
	private final Collection<List<String>> chains;
	private final Map<String, Object> filterAttributes;

	private QueryResult(RecordScheme recordScheme, List<Record> records, Record totals, int totalCount,
	                    List<String> attributes, List<String> measures, List<String> sortedBy,
	                    Collection<Drilldown> drilldowns, Collection<List<String>> chains, Map<String, Object> filterAttributes) {
		this.recordScheme = recordScheme;
		this.records = records;
		this.totals = totals;
		this.totalCount = totalCount;
		this.drilldowns = drilldowns;
		this.chains = chains;
		this.attributes = attributes;
		this.measures = measures;
		this.sortedBy = sortedBy;
		this.filterAttributes = filterAttributes;
	}

	public static QueryResult create(RecordScheme recordScheme, List<Record> records, Record totals, int totalCount,
	                                 List<String> attributes, List<String> measures, List<String> sortedBy,
	                                 Collection<Drilldown> drilldowns, Collection<List<String>> chains, Map<String, Object> filterAttributes) {
		return new QueryResult(recordScheme, records, totals, totalCount, attributes, measures, sortedBy, drilldowns, chains, filterAttributes);
	}

	public RecordScheme getRecordScheme() {
		return recordScheme;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public List<Record> getRecords() {
		return records;
	}

	public Record getTotals() {
		return totals;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public Collection<Drilldown> getDrilldowns() {
		return drilldowns;
	}

	public Collection<List<String>> getChains() {
		return chains;
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
				.add("count", totalCount)
				.add("drillDowns", drilldowns)
				.add("chains", chains)
				.add("measures", measures)
				.add("sortedBy", sortedBy)
				.toString();
	}

	public static final class Drilldown {
		private final List<String> chain;
		private final Set<String> measures;

		private Drilldown(List<String> chain, Set<String> measures) {
			this.chain = chain;
			this.measures = measures;
		}

		public static Drilldown create(List<String> chain, Set<String> measures) {return new Drilldown(chain, measures);}

		public List<String> getChain() {
			return chain;
		}

		public Set<String> getMeasures() {
			return measures;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Drilldown drilldown1 = (Drilldown) o;
			return Objects.equals(chain, drilldown1.chain) &&
					Objects.equals(measures, drilldown1.measures);
		}

		@Override
		public int hashCode() {
			return Objects.hash(chain, measures);
		}

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("chain", chain)
					.add("measures", measures)
					.toString();
		}
	}
}
