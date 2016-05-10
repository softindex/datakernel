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
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.async.ParseException;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AbstractAsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.jmx.JmxAttribute;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ReportingServiceServlet extends AbstractAsyncServlet {
	private final HttpRequestHandler handler;
	private final LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache;

	public ReportingServiceServlet(Eventloop eventloop, Cube cube,
	                               LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache) {
		super(eventloop);
		Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.Predicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.registerTypeAdapter(CubeQuery.Ordering.class, new QueryOrderingGsonSerializer())
				.create();
		this.classLoaderCache = classLoaderCache;
		this.handler = new HttpRequestHandler(gson, cube, eventloop, classLoaderCache);
	}

	@Override
	protected void doServeAsync(HttpRequest request, Callback callback) throws ParseException {
		handler.process(request, callback);
	}

	@JmxAttribute
	public Map<String, Integer> getCachedClassesCountByKey() {
		Map<String, Integer> map = new LinkedHashMap<>(classLoaderCache.getCurrentCacheSize());

		for (Map.Entry<ClassLoaderCacheKey, DefiningClassLoader> entry : classLoaderCache.asMap().entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().getDefinedClassesCount());
		}

		return map;
	}

	@JmxAttribute
	public Map<String, Map<String, String>> getCachedClassesByKey() {
		Map<String, Map<String, String>> map = new LinkedHashMap<>(classLoaderCache.getCurrentCacheSize());

		for (Map.Entry<ClassLoaderCacheKey, DefiningClassLoader> entry : classLoaderCache.asMap().entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().getDefinedClasses());
		}

		return map;
	}

	@JmxAttribute
	public Map<String, Map<String, Integer>> getCachedClassesTypesByKey() {
		Map<String, Map<String, Integer>> map = new LinkedHashMap<>(classLoaderCache.getCurrentCacheSize());

		for (Map.Entry<ClassLoaderCacheKey, DefiningClassLoader> entry : classLoaderCache.asMap().entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().getDefinedClassesByType());
		}

		return map;
	}
}
