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
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.http.HttpMethod.GET;

public final class ReportingServiceServlet implements AsyncServlet {
	protected final Logger logger = LoggerFactory.getLogger(ReportingServiceServlet.class);

	private final LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache;

	private final HttpRequestProcessor httpRequestProcessor;
	private final RequestExecutor requestExecutor;
	private final HttpResultProcessor httpResultProcessor;

	private ReportingServiceServlet(Eventloop eventloop, Cube cube,
	                                LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache) {
		super();
		Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.Predicates.class, QueryPredicatesGsonSerializer.create(cube.getStructure()))
				.registerTypeAdapter(CubeQuery.Ordering.class, QueryOrderingGsonSerializer.create())
				.create();
		this.classLoaderCache = classLoaderCache;
		this.httpRequestProcessor = HttpRequestProcessor.create(gson);
		this.requestExecutor = RequestExecutor.create(cube, cube.getStructure(), cube.getReportingConfiguration(),
				eventloop, Resolver.create(cube.getResolvers()), classLoaderCache);
		this.httpResultProcessor = HttpResultProcessor.create(cube.getStructure(), cube.getReportingConfiguration());
	}

	public static ReportingServiceServlet create(Eventloop eventloop, Cube cube,
	                                             LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache) {
		return new ReportingServiceServlet(eventloop, cube, classLoaderCache);
	}

	public static MiddlewareServlet createRootServlet(Eventloop eventloop, Cube cube, int classLoaderCacheSize) {
		LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache = LRUCache.create(classLoaderCacheSize);
		ReportingServiceServlet reportingServiceServlet = create(eventloop, cube, classLoaderCache);
		return MiddlewareServlet.create()
				.with(GET, "/", reportingServiceServlet)
				.with(GET, "/consolidation-debug", ConsolidationDebugServlet.create(cube));
	}

	@Override
	public void serve(final HttpRequest httpRequest, final ResultCallback<HttpResponse> callback) {
		logger.info("Received request: {}", httpRequest);
		try {
			final Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			final ReportingQuery reportingQuery = httpRequestProcessor.apply(httpRequest);
			requestExecutor.execute(reportingQuery, new ResultCallback<QueryResult>() {
				@Override
				protected void onResult(QueryResult result) {
					Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
					HttpResponse httpResponse = httpResultProcessor.apply(result);
					logger.info("Processed request {} ({}) [totalTime={}, jsonConstruction={}]", httpRequest,
							reportingQuery, totalTimeStopwatch, resultProcessingStopwatch);
					callback.setResult(httpResponse);
				}

				@Override
				protected void onException(Exception e) {
					logger.error("Executing query {} failed.", reportingQuery, e);
					callback.setException(e);
				}
			});
		} catch (ParseException e) {
			logger.error("Parse exception: " + httpRequest, e);
			callback.setException(e);
		} catch (QueryException e) {
			logger.error("Query exception: " + httpRequest, e);
			callback.setException(e);
		}
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
