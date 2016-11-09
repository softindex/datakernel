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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.aggregation_db.AggregationPredicate;
import io.datakernel.cube.AggregationPredicateGsonAdapter;
import io.datakernel.cube.CubeQuery;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

public class CommonUtils {
	// Reflection
	public static Object getFieldValue(String fieldName, Object obj) {
		Field field = getField(fieldName, obj.getClass());
		return getFieldValue(field, obj);
	}

	public static Object getFieldValue(Field field, Object obj) {
		try {
			return field.get(obj);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static Field getField(String fieldName, Class<?> c) {
		try {
			return c.getField(fieldName);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		}
	}

	// Codegen

//	@SuppressWarnings("unchecked")
//	public static StreamConsumers.ToList queryCube(Class<?> resultClass, CubeQuery query, Cube cube,
//	                                               Eventloop eventloop) throws QueryException {
//		StreamConsumers.ToList consumerStream = StreamConsumers.toList(eventloop);
//		cube.query(resultClass, query).streamTo(consumerStream);
//		return consumerStream;
//	}

	public static boolean nullOrContains(Set<String> set, String s) {
		return set == null || set.contains(s);
	}

	public static <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c) {
		List<T> list = new ArrayList<>(c);
		Collections.sort(list);
		return list;
	}

	public static Object instantiate(Class<?> clazz) {
		try {
			return clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static GsonBuilder createGsonBuilder(final Map<String, Type> attributeTypes, final Map<String, Type> measureTypes) {
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
