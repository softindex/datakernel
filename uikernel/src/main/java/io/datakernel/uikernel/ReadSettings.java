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

import com.google.gson.Gson;

import java.util.*;

public final class ReadSettings {
	private List<String> fields = new ArrayList<>();
	private int offset = 0;
	private int limit = Integer.MAX_VALUE;
	private Map<String, String> filters = new HashMap<>();
	private Map<String, String> sort = new HashMap<>();
	private Set<Integer> extra = new HashSet<>();

	public static ReadSettings parse(Gson gson, String json) {
		json = "{" + json.replace("&", ",") + "}";
		return gson.fromJson(json, ReadSettings.class);
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public Map<String, String> getFilters() {
		return filters;
	}

	public void setFilters(Map<String, String> filters) {
		this.filters = filters;
	}

	public Map<String, String> getSort() {
		return sort;
	}

	public void setSort(Map<String, String> sort) {
		this.sort = sort;
	}

	public Set<Integer> getExtra() {
		return extra;
	}

	public void setExtra(Set<Integer> extra) {
		this.extra = extra;
	}
}