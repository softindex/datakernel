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

package io.datakernel.cube.http;

import com.google.common.base.Splitter;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.cube.CubeQuery;
import io.datakernel.exception.ParseException;
import io.datakernel.utils.GsonAdapters;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class Utils {
	static final String MEASURES_PARAM = "measures";
	static final String ATTRIBUTES_PARAM = "attributes";
	static final String WHERE_PARAM = "where";
	static final String HAVING_PARAM = "having";
	static final String SORT_PARAM = "sort";
	static final String LIMIT_PARAM = "limit";
	static final String OFFSET_PARAM = "offset";
	static final String REPORT_TYPE_PARAM = "reportType";

	static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings();

	static String formatOrderings(List<CubeQuery.Ordering> orderings) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (CubeQuery.Ordering ordering : orderings) {
			sb.append((first ? "" : ",") +
					ordering.getField() + ":" + (ordering.isAsc() ? "ASC" : "DESC"));
			first = false;
		}
		return sb.toString();
	}

	static List<CubeQuery.Ordering> parseOrderings(String string) throws ParseException {
		List<CubeQuery.Ordering> result = new ArrayList<>();
		for (String s : SPLITTER.split(string)) {
			int i = s.indexOf(':');
			if (i == -1) {
				throw new ParseException();
			}
			String field = s.substring(0, i);
			String tail = s.substring(i + 1).toLowerCase();
			if ("asc".equals(tail))
				result.add(CubeQuery.Ordering.asc(field));
			else if ("desc".equals(tail))
				result.add(CubeQuery.Ordering.desc(field));
			else
				throw new ParseException();
		}
		return result;
	}

	public static GsonAdapters.TypeAdapterRegistryImpl createCubeTypeAdaptersRegistry() {
		return GsonAdapters.TypeAdapterRegistryImpl.create()
				.with(LocalDate.class, new TypeAdapter<LocalDate>() {
					@Override
					public void write(JsonWriter out, LocalDate value) throws IOException {
						out.value(value.toString());
					}

					@Override
					public LocalDate read(JsonReader in) throws IOException {
						return LocalDate.parse(in.nextString());
					}
				});
	}
}
