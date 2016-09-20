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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import io.datakernel.exception.ParseException;

import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.decodeDecimal;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

@SuppressWarnings("unused")
public final class ReadSettings<K> {
	public enum SortOrder {
		ASCENDING, DESCENDING;

		static SortOrder of(String value) {
			if (value.equals("asc"))
				return ASCENDING;
			else
				return DESCENDING;
		}
	}

	private static final int DEFAULT_OFFSET = 0;
	private static final int DEFAULT_LIMIT = Integer.MAX_VALUE;
	private static final Type LIST_STRING_TYPE_TOKEN = new TypeToken<List<String>>() {}.getType();
	private static final Type MAP_STRING_STRING_TYPE_TOKEN = new TypeToken<LinkedHashMap<String, String>>() {}.getType();

	private final List<String> fields;
	private final int offset;
	private final int limit;
	private final Map<String, String> filters;
	private final Map<String, SortOrder> sort;
	private final Set<K> extra;

	private ReadSettings(List<String> fields,
	                     int offset,
	                     int limit,
	                     Map<String, String> filters,
	                     Map<String, SortOrder> sort,
	                     Set<K> extra) {
		this.fields = fields;
		this.offset = offset;
		this.limit = limit;
		this.filters = filters;
		this.sort = sort;
		this.extra = extra;
	}

	public static <K> ReadSettings<K> from(Gson gson, Map<String, String> parameters) throws ParseException {

		String fieldsParameter = parameters.get("fields");
		List<String> fields;
		if (fieldsParameter != null && !fieldsParameter.isEmpty()) {
			fields = gson.fromJson(fieldsParameter, LIST_STRING_TYPE_TOKEN);
		} else {
			fields = Collections.emptyList();
		}

		String offsetParameter = parameters.get("offset");
		int offset = DEFAULT_OFFSET;
		if (offsetParameter != null && !offsetParameter.isEmpty()) {
			offset = decodeDecimal(encodeAscii(offsetParameter), 0, offsetParameter.length());
		}

		String limitParameter = parameters.get("limit");
		int limit = DEFAULT_LIMIT;
		if (limitParameter != null && !limitParameter.isEmpty()) {
			limit = decodeDecimal(encodeAscii(limitParameter), 0, limitParameter.length());
		}

		String filtersParameter = parameters.get("filters");
		Map<String, String> filters;
		if (filtersParameter != null && !filtersParameter.isEmpty()) {
			filters = gson.fromJson(filtersParameter, MAP_STRING_STRING_TYPE_TOKEN);
			filters = Collections.unmodifiableMap(filters);
		} else {
			filters = Collections.emptyMap();
		}

		String sortParameter = parameters.get("sort");
		Map<String, SortOrder> sort;
		if (sortParameter != null && !sortParameter.isEmpty()) {
			sort = new LinkedHashMap<>();
			JsonArray array = gson.fromJson(sortParameter, JsonArray.class);
			String key;
			SortOrder value;
			for (JsonElement element : array) {
				JsonArray arr = element.getAsJsonArray();
				key = arr.get(0).getAsString();

				value = SortOrder.of(arr.get(1).getAsString());
				sort.put(key, value);
			}
			sort = Collections.unmodifiableMap(sort);
		} else {
			sort = Collections.emptyMap();
		}

		String extraParameter = parameters.get("extra");
		Set<K> extra;
		if (extraParameter != null && !extraParameter.isEmpty()) {
			extra = gson.fromJson(extraParameter, new TypeToken<LinkedHashSet<K>>() {}.getType());
		} else {
			extra = Collections.emptySet();
		}

		return new ReadSettings<>(fields, offset, limit, filters, sort, extra);
	}

	public static <K> ReadSettings<K> of(List<String> fields,
	                                     int offset,
	                                     int limit,
	                                     Map<String, String> filters,
	                                     Map<String, SortOrder> sort,
	                                     Set<K> extra) {
		return new ReadSettings<>(fields, offset, limit, filters, sort, extra);
	}

	public List<String> getFields() {
		return fields;
	}

	public int getOffset() {
		return offset;
	}

	public int getLimit() {
		return limit;
	}

	public Map<String, String> getFilters() {
		return filters;
	}

	public Map<String, SortOrder> getSort() {
		return sort;
	}

	public Set<K> getExtra() {
		return extra;
	}

	@Override
	public String toString() {
		return "ReadSettings{" +
				"fields=" + fields +
				", offset=" + offset +
				", limit=" + limit +
				", filters=" + filters +
				", sort=" + sort +
				", extra=" + extra +
				'}';
	}
}