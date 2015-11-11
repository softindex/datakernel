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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.cube.api.CommonUtils.*;

public final class DimensionsRequestHandler implements AsyncHttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(DimensionsRequestHandler.class);

	private final Gson gson;
	private final Cube cube;
	private final NioEventloop eventloop;
	private final DefiningClassLoader classLoader;

	public DimensionsRequestHandler(Gson gson, Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader) {
		this.gson = gson;
		this.cube = cube;
		this.eventloop = eventloop;
		this.classLoader = classLoader;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void serveAsync(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
		logger.info("Got request {} for dimensions.", request);
		final Stopwatch sw = Stopwatch.createStarted();
		String predicatesJson = request.getParameter("filters");
		String measuresJson = request.getParameter("measures");
		final String dimension = request.getParameter("dimension");

		AggregationQuery.QueryPredicates queryPredicates = gson.fromJson(predicatesJson, AggregationQuery.QueryPredicates.class);
		List<String> measures = getListOfStrings(gson, measuresJson);
		List<String> chain = cube.buildDrillDownChain(queryPredicates.keys(), dimension);
		final Set<String> childrenDimensions = cube.findChildrenDimensions(dimension);
		List<AggregationQuery.QueryPredicate> filteredPredicates = newArrayList(Iterables.filter(queryPredicates.asCollection(), new Predicate<AggregationQuery.QueryPredicate>() {
			@Override
			public boolean apply(AggregationQuery.QueryPredicate predicate) {
				return !childrenDimensions.contains(predicate.key) && !predicate.key.equals(dimension);
			}
		}));
		List<String> predicateKeys = newArrayList(Iterables.transform(filteredPredicates, new Function<AggregationQuery.QueryPredicate, String>() {
			@Override
			public String apply(AggregationQuery.QueryPredicate queryPredicate) {
				return queryPredicate.key;
			}
		}));
		Set<String> availableMeasures = cube.getAvailableMeasures(newArrayList(Iterables.concat(chain, predicateKeys)), measures);

		final AggregationQuery query = new AggregationQuery()
				.keys(chain)
				.fields(newArrayList(availableMeasures))
				.predicates(filteredPredicates);

		final Class<?> resultClass = cube.getStructure().createResultClass(query);
		final StreamConsumers.ToList consumerStream = queryCube(resultClass, query, cube, eventloop);

		consumerStream.setResultCallback(new ResultCallback<List>() {
			@Override
			public void onResult(List result) {
				String jsonResult = constructDimensionsJson(cube, resultClass, result, query, classLoader);
				callback.onResult(createResponse(jsonResult));
				logger.info("Sent response to /dimensions request {} (query: {}) in {}", request, query, sw);
			}

			@Override
			public void onException(Exception e) {
				callback.onResult(response500(e));
				logger.error("Sending response to /dimensions query failed. Constructed query: {}", query, e);
			}
		});
	}

	public static <T> String constructDimensionsJson(Cube cube, Class<?> resultClass, List<T> results, AggregationQuery query,
	                                                 DefiningClassLoader classLoader) {
		List<String> resultKeys = query.getResultKeys();
		JsonArray jsonResults = new JsonArray();
		AggregationStructure structure = cube.getStructure();

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

			jsonResults.add(resultJsonObject);
		}

		return jsonResults.toString();
	}
}
