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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationException;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.gson.AggregationQueryGsonSerializer;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeDate;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.AvailableDrillDowns;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.*;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.stream.StreamConsumers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.util.ByteBufStrings.wrapUTF8;

public final class HttpJsonApiServer {
	private static final Logger logger = LoggerFactory.getLogger(HttpJsonApiServer.class);
	private static final String DIMENSIONS_REQUEST_PATH = "/dimensions/";
	private static final String QUERY_REQUEST_PATH = "/";
	private static final String INFO_REQUEST_PATH = "/info/";

	/**
	 * Creates an HTTP server, that runs in the specified event loop, processes JSON requests to the given cube,
	 * and listens on the specified port.
	 *
	 * @param cube      cube to query
	 * @param eventloop event loop, in which HTTP server is to run
	 * @return server instance (not started)
	 */
	public static AsyncHttpServer httpServer(Cube cube, NioEventloop eventloop) {
		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.class, new AggregationQueryGsonSerializer())
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.create();

		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.get(INFO_REQUEST_PATH, infoRequestHandler(gson, cube));

		servlet.get(QUERY_REQUEST_PATH, queryHandler(gson, cube, eventloop));

		servlet.get(DIMENSIONS_REQUEST_PATH, dimensionsRequestHandler(gson, cube, eventloop));

		return new AsyncHttpServer(eventloop, servlet);
	}

	public static AsyncHttpServer httpServer(Cube cube, NioEventloop eventloop, int port) {
		return httpServer(cube, eventloop).setListenPort(port);
	}

	private static HttpResponse createResponse(String body) {
		return HttpResponse.create()
				.body(wrapUTF8(body))
				.header(HttpHeader.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
	}

	private static AsyncHttpServlet dimensionsRequestHandler(final Gson gson, final Cube cube, final NioEventloop eventloop) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				logger.info("Got request {} for dimensions.", request);
				String predicatesJson = request.getParameter("filters");
				String measuresJson = request.getParameter("measures");
				final String dimension = request.getParameter("dimension");

				AggregationQuery.QueryPredicates queryPredicates = gson.fromJson(predicatesJson, AggregationQuery.QueryPredicates.class);
				List<String> measures = getListOfStringsFromJsonArray(gson, measuresJson);
				List<String> chain = cube.buildDrillDownChain(queryPredicates.keys(), dimension);
				final Set<String> childrenDimensions = cube.findChildrenDimensions(dimension);
				List<AggregationQuery.QueryPredicate> filteredPredicates = newArrayList(Iterables.filter(queryPredicates.asCollection(), new Predicate<AggregationQuery.QueryPredicate>() {
					@Override
					public boolean apply(AggregationQuery.QueryPredicate predicate) {
						return !childrenDimensions.contains(predicate.key) && !predicate.key.equals(dimension);
					}
				}));

				final AggregationQuery query = new AggregationQuery()
						.keys(chain)
						.fields(measures)
						.predicates(filteredPredicates);

				Class<?> resultClass = cube.getStructure().createResultClass(query);
				final StreamConsumers.ToList<?> consumerStream = queryCube(resultClass, query, cube, eventloop);

				consumerStream.addCompletionCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						String jsonResult = constructJson(gson, cube, consumerStream.getList(), query, true);
						callback.onResult(createResponse(jsonResult));
						logger.trace("Sending response {} to /dimensions query. Constructed query: {}", jsonResult, query);
					}

					@Override
					public void onException(Exception e) {
						processException(e);
						logger.error("Sending response to /dimensions query failed. Constructed query: {}", query, e);
					}
				});
			}
		};
	}

	private static AsyncHttpServlet infoRequestHandler(final Gson gson, final Cube cube) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				logger.info("Got request {} for available drill downs.", request);
				Set<String> dimensions = getSetOfStringsFromJsonArray(gson, request.getParameter("dimensions"));
				Set<String> measures = getSetOfStringsFromJsonArray(gson, request.getParameter("measures"));
				AggregationQuery.QueryPredicates queryPredicates = gson.fromJson(request.getParameter("filters"), AggregationQuery.QueryPredicates.class);
				AvailableDrillDowns availableDrillDowns =
						cube.getAvailableDrillDowns(dimensions, queryPredicates, measures);

				String responseJson = gson.toJson(availableDrillDowns);

				callback.onResult(createResponse(responseJson));
			}
		};
	}

	private static AsyncHttpServlet queryHandler(final Gson gson, final Cube cube, final NioEventloop eventloop) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				logger.info("Got query {}", request);
				List<String> dimensions = getListOfStringsFromJsonArray(gson, request.getParameter("dimensions"));
				List<String> measures = getListOfStringsFromJsonArray(gson, request.getParameter("measures"));
				String predicatesJson = request.getParameter("filters");
				AggregationQuery.QueryPredicates queryPredicates = null;
				if (predicatesJson != null) {
					queryPredicates = gson.fromJson(predicatesJson, AggregationQuery.QueryPredicates.class);
				}

				Set<String> availableMeasures = cube.getAvailableMeasures(dimensions, measures);

				final AggregationQuery finalQuery = new AggregationQuery()
						.keys(dimensions)
						.fields(newArrayList(availableMeasures));

				if (queryPredicates != null) {
					finalQuery.predicates(queryPredicates);
				}

				Class<?> resultClass = cube.getStructure().createResultClass(finalQuery);
				final StreamConsumers.ToList<?> consumerStream = queryCube(resultClass, finalQuery, cube, eventloop);

				consumerStream.addCompletionCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						String jsonResult = constructJson(gson, cube, consumerStream.getList(), finalQuery, false);
						callback.onResult(createResponse(jsonResult));
						logger.trace("Sending response {} to query {}.", jsonResult, finalQuery);
					}

					@Override
					public void onException(Exception e) {
						processException(e);
						logger.error("Sending response to query {} failed.", finalQuery, e);
					}
				});
			}
		};
	}

	private static <T> StreamConsumers.ToList<T> queryCube(Class<T> resultClass, AggregationQuery query, Cube cube,
	                                                       NioEventloop eventloop) {
		StreamConsumers.ToList<T> consumerStream = StreamConsumers.toList(eventloop);
		cube.query(0, resultClass, query).streamTo(consumerStream);
		return consumerStream;
	}

	private static Set<String> getSetOfStringsFromJsonArray(Gson gson, String json) {
		Type type = new TypeToken<Set<String>>() {
		}.getType();
		return gson.fromJson(json, type);
	}

	private static List<String> getListOfStringsFromJsonArray(Gson gson, String json) {
		Type type = new TypeToken<List<String>>() {
		}.getType();
		return gson.fromJson(json, type);
	}

	private static <T> String constructJson(Gson gson, Cube cube, List<T> results, AggregationQuery query, boolean ignoreMeasures) {
		List<String> resultKeys = query.getResultKeys();
		List<String> resultFields = query.getResultFields();
		JsonArray jsonResults = new JsonArray();
		AggregationStructure structure = cube.getStructure();

		try {
			for (T result : results) {
				Class<?> resultClass = result.getClass();
				JsonObject resultJsonObject = new JsonObject();

				for (String key : resultKeys) {
					KeyType keyType = structure.getKeyType(key);
					Field dimensionField = resultClass.getField(key);
					Object fieldValue = dimensionField.get(result);
					if (keyType instanceof KeyTypeDate) {
						String fieldValueString = keyType.toString(fieldValue);
						resultJsonObject.add(key, new JsonPrimitive(fieldValueString));
					} else {
						resultJsonObject.add(key, gson.toJsonTree(fieldValue));
					}
				}

				if (!ignoreMeasures) {
					for (String field : resultFields) {
						Field measureField = resultClass.getField(field);
						Object measureValue = measureField.get(result);
						resultJsonObject.add(field, gson.toJsonTree(measureValue));
					}
				}

				jsonResults.add(resultJsonObject);
			}
		} catch (Exception e) {
			logger.trace("Reflection exception thrown while trying to serialize fields. Query: {}. Cube: {}", query, cube, e);
		}

		return jsonResults.toString();
	}

	private static HttpResponse processException(Exception exception) {
		HttpResponse internalServerError = HttpResponse.internalServerError500();
		if (exception instanceof AggregationException) {
			internalServerError.body(wrapUTF8(exception.getMessage()));
		}
		return internalServerError;
	}
}
