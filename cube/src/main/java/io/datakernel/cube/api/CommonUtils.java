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
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpHeaders;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MediaTypes;
import io.datakernel.stream.StreamConsumers;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.codegen.Expressions.*;

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
	public static FieldGetter generateGetter(DefiningClassLoader classLoader, Class<?> objClass, String propertyName) {
		return ClassBuilder.create(classLoader, FieldGetter.class)
				.withMethod("get", getter(cast(arg(0), objClass), propertyName))
				.buildClassAndCreateNewInstance();
	}

	public static FieldSetter generateSetter(DefiningClassLoader classLoader, Class<?> objClass, String propertyName,
	                                         Class<?> propertyClass) {
		return ClassBuilder.create(classLoader, FieldSetter.class)
				.withMethod("set", setter(cast(arg(0), objClass), propertyName, cast(arg(1), propertyClass)))
				.buildClassAndCreateNewInstance();
	}

	@SuppressWarnings("unchecked")
	public static StreamConsumers.ToList queryCube(Class<?> resultClass, CubeQuery query, Cube cube,
	                                               Eventloop eventloop) throws QueryException {
		StreamConsumers.ToList consumerStream = StreamConsumers.toList(eventloop);
		cube.query(resultClass, query).streamTo(consumerStream);
		return consumerStream;
	}

	public static HttpResponse createResponse(String body) {
		HttpResponse response = HttpResponse.ok200();
		response.setContentType(ContentType.of(MediaTypes.JSON));
		response.setBody(wrapUtf8(body));
		response.addHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	public static Set<String> getSetOfStrings(Gson gson, String json) throws ParseException {
		Type type = new TypeToken<Set<String>>() {}.getType();
		return fromGson(gson, json, type);
	}

	public static List<String> getListOfStrings(Gson gson, String json) throws ParseException {
		Type type = new TypeToken<List<String>>() {}.getType();
		return fromGson(gson, json, type);
	}

	public static <T> T fromGson(Gson gson, String json, Type typeOfT) throws ParseException {
		try {
			return gson.fromJson(json, typeOfT);
		} catch (JsonSyntaxException e) {
			throw new ParseException(e);
		}
	}

	public static <T> T fromGson(Gson gson, String json, Class<T> typeOfT) throws ParseException {
		try {
			return gson.fromJson(json, typeOfT);
		} catch (JsonSyntaxException e) {
			throw new ParseException(e);
		}
	}

	public static boolean nullOrContains(Set<String> set, String s) {
		return set == null || set.contains(s);
	}

	public static <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c) {
		List<T> list = new ArrayList<>(c);
		Collections.sort(list);
		return list;
	}

	public static <T extends Comparable<? super T>> List<T> asSorted(List<T> l) {
		Collections.sort(l);
		return l;
	}

	public static Object instantiate(Class<?> clazz) {
		try {
			return clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
