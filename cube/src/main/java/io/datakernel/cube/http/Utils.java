/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.cube.http;

import io.datakernel.cube.CubeQuery;
import io.datakernel.exception.ParseException;
import io.datakernel.util.gson.GsonAdapters.TypeAdapterMapping;
import io.datakernel.util.gson.GsonAdapters.TypeAdapterMappingImpl;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static io.datakernel.util.gson.GsonAdapters.LOCAL_DATE_JSON;
import static io.datakernel.util.gson.GsonAdapters.PRIMITIVES_MAP;
import static java.util.stream.Collectors.toList;

class Utils {
	public static final ParseException MALFORMED_TAIL = new ParseException(Utils.class, "Tail is neither 'asc' nor 'desc'");
	public static final ParseException MISSING_SEMICOLON = new ParseException(Utils.class, "Failed to parse orderings, missing semicolon");

	static final String MEASURES_PARAM = "measures";
	static final String ATTRIBUTES_PARAM = "attributes";
	static final String WHERE_PARAM = "where";
	static final String HAVING_PARAM = "having";
	static final String SORT_PARAM = "sort";
	static final String LIMIT_PARAM = "limit";
	static final String OFFSET_PARAM = "offset";
	static final String REPORT_TYPE_PARAM = "reportType";

	private static final Pattern splitter = Pattern.compile(",");

	static String formatOrderings(List<CubeQuery.Ordering> orderings) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (CubeQuery.Ordering ordering : orderings) {
			sb.append(first ? "" : ",").append(ordering.getField()).append(":").append(ordering.isAsc() ? "ASC" : "DESC");
			first = false;
		}
		return sb.toString();
	}

	static List<CubeQuery.Ordering> parseOrderings(String string) throws ParseException {
		List<CubeQuery.Ordering> result = new ArrayList<>();
		List<String> tokens = splitter.splitAsStream(string)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(toList());
		for (String s : tokens) {
			int i = s.indexOf(':');
			if (i == -1) {
				throw MISSING_SEMICOLON;
			}
			String field = s.substring(0, i);
			String tail = s.substring(i + 1).toLowerCase();
			if ("asc".equals(tail))
				result.add(CubeQuery.Ordering.asc(field));
			else if ("desc".equals(tail))
				result.add(CubeQuery.Ordering.desc(field));
			else {
				throw MALFORMED_TAIL;
			}
		}
		return result;
	}

	public static TypeAdapterMapping CUBE_TYPES = TypeAdapterMappingImpl.from(PRIMITIVES_MAP)
		.withAdapter(LocalDate.class, LOCAL_DATE_JSON);
}
