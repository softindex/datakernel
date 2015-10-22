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
import io.datakernel.aggregation_db.api.QueryResultPlaceholder;
import io.datakernel.aggregation_db.api.ReportingDSLExpression;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeDate;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.AvailableDrillDowns;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.*;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.stream.StreamConsumers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.ByteBufStrings.wrapUTF8;

@SuppressWarnings("unchecked")
public final class HttpJsonApiServer {
	private static final Logger logger = LoggerFactory.getLogger(HttpJsonApiServer.class);
	private static final String DIMENSIONS_REQUEST_PATH = "/dimensions/";
	private static final String QUERY_REQUEST_PATH = "/";
	private static final String INFO_REQUEST_PATH = "/info/";
	private static final String REPORTING_QUERY_REQUEST_PATH = "/reporting/";

	/**
	 * Creates an HTTP server, that runs in the specified event loop, processes JSON requests to the given cube,
	 * and listens on the specified port.
	 *
	 * @param cube      cube to query
	 * @param eventloop event loop, in which HTTP server is to run
	 * @return server instance (not started)
	 */
	public static AsyncHttpServer httpServer(Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader) {
		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.create();

		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.get(INFO_REQUEST_PATH, infoRequestHandler(gson, cube));

		servlet.get(QUERY_REQUEST_PATH, queryHandler(gson, cube, eventloop, classLoader));

		servlet.get(DIMENSIONS_REQUEST_PATH, dimensionsRequestHandler(gson, cube, eventloop, classLoader));

		servlet.get(REPORTING_QUERY_REQUEST_PATH, reportingQueryHandler(gson, cube, eventloop, classLoader));

		return new AsyncHttpServer(eventloop, servlet);
	}

	public static AsyncHttpServer httpServer(Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader, int port) {
		return httpServer(cube, eventloop, classLoader).setListenPort(port);
	}

	private static AsyncHttpServlet dimensionsRequestHandler(final Gson gson, final Cube cube, final NioEventloop eventloop,
	                                                         final DefiningClassLoader classLoader) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				logger.info("Got request {} for dimensions.", request);
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

				final AggregationQuery query = new AggregationQuery()
						.keys(chain)
						.fields(measures)
						.predicates(filteredPredicates);

				Class<?> resultClass = cube.getStructure().createResultClass(query);
				final StreamConsumers.ToList consumerStream = queryCube(resultClass, query, cube, eventloop);

				consumerStream.setResultCallback(new ResultCallback<List>() {
					@Override
					public void onResult(List result) {
						String jsonResult = constructDimensionsJson(gson, cube, result, query, classLoader);
						callback.onResult(createResponse(jsonResult));
						logger.trace("Sending response {} to /dimensions query. Constructed query: {}", jsonResult, query);
					}

					@Override
					public void onException(Exception e) {
						callback.onResult(response500(e));
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
				Set<String> dimensions = getSetOfStrings(gson, request.getParameter("dimensions"));
				Set<String> measures = getSetOfStrings(gson, request.getParameter("measures"));
				AggregationQuery.QueryPredicates queryPredicates = gson.fromJson(request.getParameter("filters"), AggregationQuery.QueryPredicates.class);
				AvailableDrillDowns availableDrillDowns =
						cube.getAvailableDrillDowns(dimensions, queryPredicates, measures);

				String responseJson = gson.toJson(availableDrillDowns);

				callback.onResult(createResponse(responseJson));
			}
		};
	}

	private static AsyncHttpServlet queryHandler(final Gson gson, final Cube cube, final NioEventloop eventloop,
	                                             final DefiningClassLoader classLoader) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				logger.info("Got query {}", request);
				List<String> dimensions = getListOfStrings(gson, request.getParameter("dimensions"));
				Set<String> measures = getSetOfStrings(gson, request.getParameter("measures"));
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
				final StreamConsumers.ToList consumerStream = queryCube(resultClass, finalQuery, cube, eventloop);

				consumerStream.setResultCallback(new ResultCallback<List>() {
					@Override
					public void onResult(List result) {
						String jsonResult = constructQueryJson(gson, cube, result, finalQuery,
								classLoader);
						callback.onResult(createResponse(jsonResult));
						logger.trace("Sending response {} to query {}.", jsonResult, finalQuery);
					}

					@Override
					public void onException(Exception e) {
						callback.onResult(response500(e));
						logger.error("Sending response to query {} failed.", finalQuery, e);
					}
				});
			}
		};
	}

	private static AsyncHttpServlet reportingQueryHandler(final Gson gson, final Cube cube, final NioEventloop eventloop,
	                                                      final DefiningClassLoader classLoader) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				logger.info("Got query {}", request);
				final AggregationStructure structure = cube.getStructure();

				List<String> dimensions = getListOfStrings(gson, request.getParameter("dimensions"));

				for (String dimension : dimensions) {
					if (!structure.getKeys().containsKey(dimension)) {
						callback.onResult(response404("Cube does not contain dimension with name '" + dimension + "'"));
						return;
					}
				}

				final List<String> queryMeasures = getListOfStrings(gson, request.getParameter("measures"));
				Set<String> storedMeasures = newHashSet();
				List<String> computedMeasures = newArrayList();

				for (String queryMeasure : queryMeasures) {
					if (structure.getFields().containsKey(queryMeasure)) {
						storedMeasures.add(queryMeasure);
					} else if (structure.getComputedMeasures().containsKey(queryMeasure)) {
						ReportingDSLExpression reportingDSLExpression = structure.getComputedMeasures().get(queryMeasure);
						storedMeasures.addAll(reportingDSLExpression.getMeasureDependencies());
						computedMeasures.add(queryMeasure);
					} else {
						callback.onResult(response404("Cube does not contain measure with name '" + queryMeasure + "'"));
						return;
					}
				}

				String predicatesJson = request.getParameter("filters");
				String orderingsJson = request.getParameter("sort");
				String limitString = request.getParameter("limit");
				String offsetString = request.getParameter("offset");

				AggregationQuery.QueryPredicates queryPredicates = null;
				if (predicatesJson != null) {
					queryPredicates = gson.fromJson(predicatesJson, AggregationQuery.QueryPredicates.class);
				}

				List<List<String>> orderings = getListOfLists(gson, orderingsJson);

				Set<String> availableMeasures = cube.getAvailableMeasures(dimensions, storedMeasures);

				final AggregationQuery finalQuery = new AggregationQuery()
						.keys(dimensions)
						.fields(newArrayList(availableMeasures));

				if (orderings != null) {
					addOrderings(finalQuery, orderings);
				}

				if (queryPredicates != null) {
					finalQuery.predicates(queryPredicates);
				}

				Class<QueryResultPlaceholder> resultClass = structure.createResultClass(finalQuery, computedMeasures);
				final StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = queryCubeReporting(resultClass, finalQuery, cube, eventloop);

				final Integer limit = limitString == null ? null : Integer.valueOf(limitString);
				final Integer offset = offsetString == null ? null : Integer.valueOf(offsetString);

				consumerStream.setResultCallback(new ResultCallback<List<QueryResultPlaceholder>>() {
					@Override
					public void onResult(List<QueryResultPlaceholder> results) {
						for (QueryResultPlaceholder queryResult : results) {
							queryResult.compute();
						}
						String jsonResult = constructReportingQueryJson(gson, structure, queryMeasures,
								consumerStream.getList(), finalQuery, classLoader, limit, offset);
						callback.onResult(createResponse(jsonResult));
						logger.trace("Sending response {} to query {}.", jsonResult, finalQuery);
					}

					@Override
					public void onException(Exception e) {
						callback.onResult(response500(e));
						logger.error("Sending response to query {} failed.", finalQuery, e);
					}
				});
			}
		};
	}

	private static AggregationQuery addOrderings(AggregationQuery query, List<List<String>> orderings) {
		for (List<String> ordering : orderings) {
			String propertyName = ordering.get(0);
			String direction = ordering.get(1);
			if (direction.equals("asc"))
				query.orderAsc(propertyName);
			else if (direction.equals("desc"))
				query.orderDesc(propertyName);
		}
		return query;
	}

	private static StreamConsumers.ToList queryCube(Class<?> resultClass, AggregationQuery query, Cube cube,
	                                                NioEventloop eventloop) {
		StreamConsumers.ToList consumerStream = StreamConsumers.toList(eventloop);
		cube.query(resultClass, query).streamTo(consumerStream);
		return consumerStream;
	}

	private static StreamConsumers.ToList<QueryResultPlaceholder> queryCubeReporting(Class<QueryResultPlaceholder> resultClass, AggregationQuery query, Cube cube,
	                                                NioEventloop eventloop) {
		StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = StreamConsumers.toList(eventloop);
		cube.query(resultClass, query).streamTo(consumerStream);
		return consumerStream;
	}

	private static Set<String> getSetOfStrings(Gson gson, String json) {
		Type type = new TypeToken<Set<String>>() {}.getType();
		return gson.fromJson(json, type);
	}

	private static List<String> getListOfStrings(Gson gson, String json) {
		Type type = new TypeToken<List<String>>() {}.getType();
		return gson.fromJson(json, type);
	}

	private static List<List<String>> getListOfLists(Gson gson, String json) {
		Type type = new TypeToken<List<List<String>>>() {}.getType();
		return gson.fromJson(json, type);
	}

	private static <T> String constructQueryJson(Gson gson, Cube cube, List<T> results, AggregationQuery query,
	                                             DefiningClassLoader classLoader) {
		List<String> resultKeys = query.getResultKeys();
		List<String> resultFields = query.getResultFields();
		JsonArray jsonResults = new JsonArray();
		AggregationStructure structure = cube.getStructure();

		for (T result : results) {
			Class<?> resultClass = result.getClass();
			JsonObject resultJsonObject = new JsonObject();

			for (String key : resultKeys) {
				addValueOfKey(resultJsonObject, result, resultClass, key, structure, classLoader, gson);
			}

			for (String field : resultFields) {
				Object fieldValue = generateGetter(classLoader, resultClass, field).get(result);
				resultJsonObject.add(field, gson.toJsonTree(fieldValue));
			}

			jsonResults.add(resultJsonObject);
		}

		return jsonResults.toString();
	}

	private static <T> String constructReportingQueryJson(Gson gson, AggregationStructure structure, List<String> resultFields,
	                                                      List<T> results, AggregationQuery query,
	                                                      DefiningClassLoader classLoader, Integer limit, Integer offset) {
		List<String> resultKeys = query.getResultKeys();
		JsonObject jsonResult = new JsonObject();
		JsonArray jsonRecords = new JsonArray();

		int start = offset == null ? 0 : offset;
		int end;

		if (limit == null)
			end = results.size();
		else if (start + limit > results.size())
			end = results.size();
		else
			end = start + limit;

		for (int i = start; i < end; ++i) {
			T result = results.get(i);
			Class<?> resultClass = result.getClass();
			JsonObject resultJsonObject = new JsonObject();

			for (String key : resultKeys) {
				addValueOfKey(resultJsonObject, result, resultClass, key, structure, classLoader, gson);
			}

			for (String field : resultFields) {
				Object fieldValue = generateGetter(classLoader, resultClass, field).get(result);
				resultJsonObject.add(field, gson.toJsonTree(fieldValue));
			}

			jsonRecords.add(resultJsonObject);
		}

		jsonResult.add("records", jsonRecords);
		jsonResult.addProperty("count", results.size());

		return jsonResult.toString();
	}

	private static <T> String constructDimensionsJson(Gson gson, Cube cube, List<T> results, AggregationQuery query,
	                                                  DefiningClassLoader classLoader) {
		List<String> resultKeys = query.getResultKeys();
		JsonArray jsonResults = new JsonArray();
		AggregationStructure structure = cube.getStructure();

		for (T result : results) {
			Class<?> resultClass = result.getClass();
			JsonObject resultJsonObject = new JsonObject();

			for (String key : resultKeys) {
				addValueOfKey(resultJsonObject, result, resultClass, key, structure, classLoader, gson);
			}

			jsonResults.add(resultJsonObject);
		}

		return jsonResults.toString();
	}

	private static void addValueOfKey(JsonObject resultJsonObject, Object result, Class<?> resultClass, String key,
	                                  AggregationStructure structure, DefiningClassLoader classLoader, Gson gson) {
		KeyType keyType = structure.getKeyType(key);
		Object valueOfKey = generateGetter(classLoader, resultClass, key).get(result);
		if (keyType instanceof KeyTypeDate) {
			String fieldValueString = keyType.toString(valueOfKey);
			resultJsonObject.add(key, new JsonPrimitive(fieldValueString));
		} else {
			resultJsonObject.add(key, gson.toJsonTree(valueOfKey));
		}
	}

	private static FieldGetter generateGetter(DefiningClassLoader classLoader, Class<?> objClass, String propertyName) {
		AsmBuilder<FieldGetter> builder = new AsmBuilder<>(classLoader, FieldGetter.class);
		builder.method("get", field(cast(arg(0), objClass), propertyName));
		return builder.newInstance();
	}

	private static HttpResponse createResponse(String body) {
		return HttpResponse.create()
				.body(wrapUTF8(body))
				.header(HttpHeader.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
	}

	private static HttpResponse response500(Exception exception) {
		HttpResponse internalServerError = HttpResponse.internalServerError500();
		if (exception instanceof AggregationException) {
			internalServerError.body(wrapUTF8(exception.getMessage()));
		}
		return internalServerError;
	}

	private static HttpResponse response404(String message) {
		HttpResponse response404 = HttpResponse.notFound404();
		response404.body(wrapUTF8(message));
		return response404;
	}
}
