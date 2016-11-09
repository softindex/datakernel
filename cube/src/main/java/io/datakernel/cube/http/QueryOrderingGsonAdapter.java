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

import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.cube.CubeQuery;

import java.io.IOException;

import static java.lang.String.format;

final class QueryOrderingGsonAdapter extends TypeAdapter<CubeQuery.Ordering> {
	private static final String ASC = "asc";
	private static final String DESC = "desc";

	private QueryOrderingGsonAdapter() {}

	public static QueryOrderingGsonAdapter create() {return new QueryOrderingGsonAdapter();}

	@Override
	public CubeQuery.Ordering read(JsonReader in) throws IOException {
		in.beginObject();
		String field = in.nextName();
		String directionString = in.nextString();
		CubeQuery.Ordering ordering;
		if (ASC.equals(directionString))
			ordering = CubeQuery.Ordering.asc(field);
		else if (DESC.equals(directionString))
			ordering = CubeQuery.Ordering.desc(field);
		else
			throw new JsonParseException(format("Unknown '%s' property value in sort object. Should be either '%s' or '%s'", directionString, ASC, DESC));
		in.endObject();
		return ordering;
	}

	@Override
	public void write(JsonWriter out, CubeQuery.Ordering value) throws IOException {
		out.beginObject();
		out.name(value.getField());
		out.value(value.isAsc() ? ASC : DESC);
		out.endObject();
	}

}
