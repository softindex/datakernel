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

import java.util.List;
import java.util.Map;

public final class ReportingQueryResult {
	private final List<Map<String, Object>> records;
	private final Map<String, Object> totals;
	private final int count;

	public ReportingQueryResult(List<Map<String, Object>> records, Map<String, Object> totals, int count) {
		this.records = records;
		this.totals = totals;
		this.count = count;
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
}
