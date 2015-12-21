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

import java.util.*;

public class ReadSettings {
	private List<String> fields = new ArrayList<>();
	private int offset = 0;
	private int limit = Integer.MAX_VALUE;
	private Map<String, String> filters = new HashMap<>();
	private Map<String, String> sort = new HashMap<>();
	private Set<Integer> extra = new HashSet<>();

	public static ReadSettings get(Map<String, String> parameters) {
		ReadSettings settings = new ReadSettings();
		String fs = parameters.get("fields");
		if (fs != null) {
			settings.fields.addAll(Arrays.asList(fs.substring(2, fs.length() - 2).split("\",\"")));
		}
		String offset = parameters.get("offset");
		if (offset != null) {
			settings.offset = Integer.valueOf(offset);
		}
		String limit = parameters.get("limit");
		if (limit != null) {
			settings.limit = Integer.valueOf(limit);
		}
		String filters = parameters.get("filters");
		if (filters != null) {
			filters = filters.replace("\"", "").replace("{", "").replace("}", "");
			String[] arr = filters.split(",");
			for (String s : arr) {
				String[] ss = s.split(":");
				settings.filters.put(ss[0], ss.length > 1 ? ss[1] : null);
			}
		}
		String sort = parameters.get("sort");
		if (sort != null) {
			sort = sort.replace("],[", "\t");
			String[] sorts = sort.split("\t");
			for (String s : sorts) {
				s = s.replace("\"", "").replace("[", "").replace("]", "");
				String[] ss = s.split(",");
				settings.sort.put(ss[0], ss[1]);
			}
		}
		String extra = parameters.get("extra");
		if (extra != null) {
			extra = extra.replace("[", "").replace("]", "");
			String[] es = extra.split(",");
			for (String e : es) {
				if (!e.isEmpty()) {
					e = e.replace("\"", "");
					settings.extra.add(Integer.valueOf(e));
				}
			}
		}
		return settings;
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