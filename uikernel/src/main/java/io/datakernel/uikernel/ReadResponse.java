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

package io.datakernel.uikernel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ReadResponse {
	private final List<List> records;
	private List<List> extra;
	private Object totals;
	private final int count;

	public ReadResponse(List<List> records, int count) {
		this(records, null, null, count);
	}

	public ReadResponse(List<List> records, List<List> extra, Object totals, int count) {
		this.records = records;
		this.extra = extra;
		this.totals = totals;
		this.count = count;
	}

	public void setExtra(List<List> extra) {
		this.extra = extra;
	}

	public void setTotals(Object totals) {
		this.totals = totals;
	}

	Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("records", records);
		if (extra != null) map.put("extra", extra);
		if (totals != null) map.put("totals", totals);
		map.put("count", count);
		return map;
	}
}