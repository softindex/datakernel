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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.datakernel.cube.api.CommonUtils.*;

public final class QueryHandler implements AsyncHttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(QueryHandler.class);

	private final Gson gson;
	private final Cube cube;
	private final NioEventloop eventloop;
	private final DefiningClassLoader classLoader;

	public QueryHandler(Gson gson, Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader) {
		this.gson = gson;
		this.cube = cube;
		this.eventloop = eventloop;
		this.classLoader = classLoader;
	}

	@Override
	public void serveAsync(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
		logger.info("Got query {}", request);
		final Stopwatch sw = Stopwatch.createStarted();
		List<String> dimensions = getListOfStrings(gson, request.getParameter("dimensions"));
		List<String> measures = getListOfStrings(gson, request.getParameter("measures"));
		String predicatesJson = request.getParameter("filters");

		AggregationQuery.QueryPredicates queryPredicates = null;
		if (predicatesJson != null) {
			queryPredicates = gson.fromJson(predicatesJson, AggregationQuery.QueryPredicates.class);
		}

		final AggregationQuery finalQuery = new AggregationQuery()
				.keys(dimensions)
				.fields(measures);

		if (queryPredicates != null) {
			finalQuery.predicates(queryPredicates);
		}

		final Class<?> resultClass = cube.getStructure().createResultClass(finalQuery);
		final StreamConsumers.ToList consumerStream = queryCube(resultClass, finalQuery, cube, eventloop);

		consumerStream.setResultCallback(new ResultCallback<List>() {
			@Override
			public void onResult(List result) {
				String jsonResult = constructQueryResultJson(cube, resultClass, result, finalQuery,
						classLoader);
				callback.onResult(createResponse(jsonResult));
				logger.info("Sent response to request {} (query: {}) in {}", request, finalQuery, sw);
			}

			@Override
			public void onException(Exception e) {
				callback.onResult(response500(e));
				logger.error("Sending response to query {} failed.", finalQuery, e);
			}
		});
	}

	private static <T> String constructQueryResultJson(Cube cube, Class<?> resultClass, List<T> results, AggregationQuery query,
	                                                   DefiningClassLoader classLoader) {
		List<String> resultKeys = query.getResultKeys();
		List<String> resultFields = query.getResultFields();
		JsonArray jsonResults = new JsonArray();
		AggregationStructure structure = cube.getStructure();

		FieldGetter[] fieldGetters = new FieldGetter[resultFields.size()];
		for (int i = 0; i < resultFields.size(); i++) {
			String field = resultFields.get(i);
			fieldGetters[i] = generateGetter(classLoader, resultClass, field);
		}

		FieldGetter[] keyGetters = new FieldGetter[resultKeys.size()];
		KeyType[] keyTypes = new KeyType[resultKeys.size()];
		for (int i = 0; i < resultKeys.size(); i++) {
			String key = resultKeys.get(i);
			keyGetters[i] = generateGetter(classLoader, resultClass, key);
			keyTypes[i] = structure.getKeyType(key);
		}

		for (T result : results) {
			JsonObject resultJsonObject = new JsonObject();

			for (int i = 0; i < resultKeys.size(); i++) {
				resultJsonObject.add(resultKeys.get(i), keyTypes[i].toJson(keyGetters[i].get(result)));
			}

			for (int i = 0; i < resultFields.size(); i++) {
				resultJsonObject.add(resultFields.get(i), new JsonPrimitive((Number) fieldGetters[i].get(result)));
			}

			jsonResults.add(resultJsonObject);
		}

		return jsonResults.toString();
	}
}
