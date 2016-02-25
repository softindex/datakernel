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
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationException;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpHeaders;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MediaTypes;
import io.datakernel.stream.StreamConsumers;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.ByteBufStrings.wrapUTF8;

public class CommonUtils {
	public static FieldGetter generateGetter(DefiningClassLoader classLoader, Class<?> objClass, String propertyName) {
		return new AsmBuilder<>(classLoader, FieldGetter.class)
				.method("get", getter(cast(arg(0), objClass), propertyName))
				.newInstance();
	}

	public static FieldSetter generateSetter(DefiningClassLoader classLoader, Class<?> objClass, String propertyName,
	                                         Class<?> propertyClass) {
		return new AsmBuilder<>(classLoader, FieldSetter.class)
				.method("set", setter(cast(arg(0), objClass), propertyName, cast(arg(1), propertyClass)))
				.newInstance();
	}

	@SuppressWarnings("unchecked")
	public static StreamConsumers.ToList queryCube(Class<?> resultClass, CubeQuery query, Cube cube,
	                                               Eventloop eventloop) {
		StreamConsumers.ToList consumerStream = StreamConsumers.toList(eventloop);
		cube.query(resultClass, query).streamTo(consumerStream);
		return consumerStream;
	}

	public static HttpResponse createResponse(String body) {
		return HttpResponse.create()
				.contentType(ContentType.of(MediaTypes.JSON))
				.body(wrapUTF8(body))
				.header(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
	}

	public static HttpResponse response500(Exception exception) {
		HttpResponse internalServerError = HttpResponse.internalServerError500();
		if (exception instanceof AggregationException) {
			internalServerError.body(wrapUTF8(exception.getMessage()));
		}
		return internalServerError;
	}

	public static HttpResponse response500(String message) {
		HttpResponse response500 = HttpResponse.internalServerError500();
		response500.body(wrapUTF8(message));
		return response500;
	}

	public static HttpResponse response400(String message) {
		HttpResponse response400 = HttpResponse.badRequest400();
		response400.body(wrapUTF8(message));
		return response400;
	}

	public static HttpResponse response404(String message) {
		HttpResponse response404 = HttpResponse.notFound404();
		response404.body(wrapUTF8(message));
		return response404;
	}

	public static Set<String> getSetOfStrings(Gson gson, String json) {
		Type type = new TypeToken<Set<String>>() {}.getType();
		return gson.fromJson(json, type);
	}

	public static List<String> getListOfStrings(Gson gson, String json) {
		Type type = new TypeToken<List<String>>() {}.getType();
		return gson.fromJson(json, type);
	}

	public static boolean nullOrContains(Set<String> set, String s) {
		return set == null || set.contains(s);
	}

	public static Object instantiate(Class<?> clazz) {
		try {
			return clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
