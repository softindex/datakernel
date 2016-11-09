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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.QueryResult;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

class Utils {
	static final String MEASURES_PARAM = "measures";
	static final String ATTRIBUTES_PARAM = "attributes";
	static final String FILTERS_PARAM = "filters";
	static final String HAVING_PARAM = "having";
	static final String SORT_PARAM = "sort";
	static final String LIMIT_PARAM = "limit";
	static final String OFFSET_PARAM = "offset";

	static GsonBuilder createGsonBuilder(final Map<String, Type> attributeTypes, final Map<String, Type> measureTypes) {
		return new GsonBuilder()
				.registerTypeAdapterFactory(new TypeAdapterFactory() {
					@SuppressWarnings("unchecked")
					@Override
					public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
						if (AggregationPredicate.class.isAssignableFrom(type.getRawType())) {
							return (TypeAdapter<T>) AggregationPredicateGsonAdapter.create(gson, attributeTypes, measureTypes);
						}
						if (type.getRawType() == QueryResult.class) {
							return (TypeAdapter<T>) QueryResultGsonAdapter.create(gson, attributeTypes, measureTypes);
						}
						return null;
					}
				})
				.registerTypeAdapter(LocalDate.class, new TypeAdapter<LocalDate>() {
					@Override
					public void write(JsonWriter out, LocalDate value) throws IOException {
						out.value(value.toString());
					}

					@Override
					public LocalDate read(JsonReader in) throws IOException {
						return LocalDate.parse(in.nextString());
					}
				})
				.registerTypeAdapter(CubeQuery.Ordering.class, QueryOrderingGsonAdapter.create());
	}

}
