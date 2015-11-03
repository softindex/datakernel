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
import com.google.common.net.MediaType;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationException;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.api.QueryResultPlaceholder;
import io.datakernel.aggregation_db.api.ReportingDSLExpression;
import io.datakernel.aggregation_db.api.TotalsPlaceholder;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.ExpressionComparator;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.AvailableDrillDowns;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.*;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
				Set<String> availableMeasures = cube.getAvailableMeasures(chain, measures);

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
						String jsonResult = constructQueryJson(cube, resultClass, result, finalQuery,
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
					if (!structure.containsKey(dimension)) {
						callback.onResult(response404("Cube does not contain dimension with name '" + dimension + "'"));
						return;
					}
				}

				final List<String> queryMeasures = getListOfStrings(gson, request.getParameter("measures"));
				final Set<String> storedMeasures = newHashSet();
				final Set<String> computedMeasures = newHashSet();

				for (String queryMeasure : queryMeasures) {
					if (structure.containsOutputField(queryMeasure)) {
						storedMeasures.add(queryMeasure);
					} else if (structure.containsComputedMeasure(queryMeasure)) {
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

				final AggregationQuery finalQuery = new AggregationQuery()
						.keys(dimensions)
						.fields(newArrayList(storedMeasures));

				List<String> ordering = getListOfStrings(gson, orderingsJson);
				boolean orderingByComputedMeasure = false;
				String orderingField = null;
				boolean ascendingOrdering = false;

				if (ordering != null) {
					if (ordering.size() != 2) {
						callback.onResult(response500("Incorrect 'sort' parameter format"));
						return;
					}
					orderingField = ordering.get(0);
					orderingByComputedMeasure = computedMeasures.contains(orderingField);

					if (!dimensions.contains(orderingField) && !storedMeasures.contains(orderingField) && !orderingByComputedMeasure) {
						callback.onResult(response500("Incorrect field specified in 'sort' parameter"));
						return;
					}

					String direction = ordering.get(1);

					if (direction.equals("asc"))
						ascendingOrdering = true;
					else if (direction.equals("desc"))
						ascendingOrdering = false;
					else {
						callback.onResult(response500("Incorrect ordering specified in 'sort' parameter"));
						return;
					}

					addOrdering(finalQuery, orderingField, ascendingOrdering, computedMeasures);
				}

				if (queryPredicates != null) {
					finalQuery.predicates(queryPredicates);
				}

				final Class<QueryResultPlaceholder> resultClass = createResultClass(classLoader, finalQuery, computedMeasures, structure);
				final StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = queryCubeReporting(resultClass, finalQuery, cube, eventloop);

				final Integer limit = limitString == null ? null : Integer.valueOf(limitString);
				final Integer offset = offsetString == null ? null : Integer.valueOf(offsetString);

				final boolean sortingRequired = orderingByComputedMeasure;
				Comparator comparator = null;
				if (sortingRequired) {
					comparator = generateComparator(classLoader, orderingField, ascendingOrdering, resultClass);
				}
				final Comparator finalComparator = comparator;

				consumerStream.setResultCallback(new ResultCallback<List<QueryResultPlaceholder>>() {
					@Override
					public void onResult(List<QueryResultPlaceholder> results) {
						if (results.isEmpty()) {
							callback.onResult(createResponse(""));
							return;
						}

						// compute totals
						List<String> requestedStoredMeasures = newArrayList(Iterables.filter(finalQuery.getResultFields(), new Predicate<String>() {
							@Override
							public boolean apply(String queryMeasure) {
								return !computedMeasures.contains(queryMeasure);
							}
						}));
						TotalsPlaceholder totalsPlaceholder = createTotalsPlaceholder(classLoader, structure, resultClass, storedMeasures, computedMeasures);
						totalsPlaceholder.initAccumulator(results.get(0));
						if (results.size() > 1) {
							for (int i = 1; i < results.size(); ++i) {
								totalsPlaceholder.accumulate(results.get(i));
							}
						}
						totalsPlaceholder.computeMeasures();

						// compute measures
						for (QueryResultPlaceholder queryResult : results) {
							queryResult.computeMeasures();
						}

						// sort
						if (sortingRequired) {
							Collections.sort(results, finalComparator);
						}

						String jsonResult = constructReportingQueryJson(gson, resultClass, structure, queryMeasures,
								consumerStream.getList(), totalsPlaceholder, finalQuery, classLoader, limit, offset);
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

	private static AggregationQuery addOrdering(AggregationQuery query, String fieldName, boolean ascendingOrdering, Set<String> computedMeasures) {
		if (computedMeasures.contains(fieldName))
			return query;

		if (ascendingOrdering)
			query.orderAsc(fieldName);
		else
			query.orderDesc(fieldName);

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

	/* JSON utils */
	private static Set<String> getSetOfStrings(Gson gson, String json) {
		Type type = new TypeToken<Set<String>>() {}.getType();
		return gson.fromJson(json, type);
	}

	private static List<String> getListOfStrings(Gson gson, String json) {
		Type type = new TypeToken<List<String>>() {}.getType();
		return gson.fromJson(json, type);
	}

	private static <T> String constructQueryJson(Cube cube, Class<?> resultClass, List<T> results, AggregationQuery query,
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

	private static <T> String constructReportingQueryJson(Gson gson, Class<?> resultClass, AggregationStructure structure, List<String> resultFields,
	                                                      List<T> results, TotalsPlaceholder totalsPlaceholder, AggregationQuery query,
	                                                      DefiningClassLoader classLoader, Integer limit, Integer offset) {
		List<String> resultKeys = query.getResultKeys();
		JsonObject jsonResult = new JsonObject();
		JsonArray jsonRecords = new JsonArray();
		JsonObject jsonTotals = new JsonObject();

		int start = offset == null ? 0 : offset;
		int end;

		if (limit == null)
			end = results.size();
		else if (start + limit > results.size())
			end = results.size();
		else
			end = start + limit;

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

		for (int i = start; i < end; ++i) {
			T result = results.get(i);
			JsonObject resultJsonObject = new JsonObject();

			for (int j = 0; j < resultKeys.size(); j++) {
				resultJsonObject.add(resultKeys.get(i), keyTypes[i].toJson(keyGetters[i].get(result)));
			}

			for (int j = 0; j < resultFields.size(); j++) {
				resultJsonObject.add(resultFields.get(i), new JsonPrimitive((Number) fieldGetters[i].get(result)));
			}

			jsonRecords.add(resultJsonObject);
		}

		for (String field : resultFields) {
			Object totalFieldValue = generateGetter(classLoader, totalsPlaceholder.getClass(), field).get(totalsPlaceholder);
			jsonTotals.addProperty(field, totalFieldValue.toString());
		}

		jsonResult.add("records", jsonRecords);
		jsonResult.add("totals", jsonTotals);
		jsonResult.addProperty("count", results.size());

		return jsonResult.toString();
	}

	private static <T> String constructDimensionsJson(Cube cube, Class<?> resultClass, List<T> results, AggregationQuery query,
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

	/* Codegen utils */
	private static Class<QueryResultPlaceholder> createResultClass(DefiningClassLoader classLoader, AggregationQuery query,
	                                                               Set<String> computedMeasureNames, AggregationStructure structure) {
		AsmBuilder<QueryResultPlaceholder> builder = new AsmBuilder<>(classLoader, QueryResultPlaceholder.class);
		List<String> resultKeys = query.getResultKeys();
		List<String> resultFields = query.getResultFields();
		for (String key : resultKeys) {
			KeyType keyType = structure.getKeyType(key);
			builder.field(key, keyType.getDataType());
		}
		for (String field : resultFields) {
			FieldType fieldType = structure.getOutputFieldType(field);
			builder.field(field, fieldType.getDataType());
		}
		ExpressionSequence computeSequence = sequence();
		for (String computedMeasure : computedMeasureNames) {
			builder.field(computedMeasure, double.class);
			computeSequence.add(set(getter(self(), computedMeasure), structure.getComputedMeasureExpression(computedMeasure)));
		}
		builder.method("computeMeasures", computeSequence);
		return builder.defineClass();
	}

	private static TotalsPlaceholder createTotalsPlaceholder(DefiningClassLoader classLoader,
	                                                         AggregationStructure structure, Class<?> inputClass,
	                                                         Set<String> requestedStoredFields, Set<String> computedMeasureNames) {
		AsmBuilder<TotalsPlaceholder> builder = new AsmBuilder<>(classLoader, TotalsPlaceholder.class);

		ExpressionSequence initAccumulatorSequence = sequence();

		for (String field : requestedStoredFields) {
			FieldType fieldType = structure.getOutputFieldType(field);
			builder.field(field, fieldType.getDataType());
		}
		for (String computedMeasure : computedMeasureNames) {
			builder.field(computedMeasure, double.class);
		}

		for (String field : requestedStoredFields) {
			initAccumulatorSequence.add(set(
					getter(self(), field),
					getter(cast(arg(0), inputClass), field)));
		}
		builder.method("initAccumulator", initAccumulatorSequence);

		ExpressionSequence accumulateSequence = sequence();
		for (String field : requestedStoredFields) {
			accumulateSequence.add(set(
					getter(self(), field),
					add(
							getter(self(), field),
							getter(cast(arg(0), inputClass), field))));
		}
		builder.method("accumulate", accumulateSequence);

		ExpressionSequence computeSequence = sequence();
		for (String computedMeasure : computedMeasureNames) {
			computeSequence.add(set(getter(self(), computedMeasure), structure.getComputedMeasureExpression(computedMeasure)));
		}
		builder.method("computeMeasures", computeSequence);

		return builder.newInstance();
	}

	private static FieldGetter generateGetter(DefiningClassLoader classLoader, Class<?> objClass, String propertyName) {
		AsmBuilder<FieldGetter> builder = new AsmBuilder<>(classLoader, FieldGetter.class);
		// TODO (dtkachenko): use getter expression instead of field expression
		// TODO (vsavchuk): implement getter and setter expressions
		builder.method("get", getter(cast(arg(0), objClass), propertyName));
		return builder.newInstance();
	}

	private static Comparator generateComparator(DefiningClassLoader classLoader, String fieldName, boolean ascending,
	                                             Class<QueryResultPlaceholder> fieldClass) {
		AsmBuilder<Comparator> builder = new AsmBuilder<>(classLoader, Comparator.class);
		ExpressionComparator comparator = comparator();
		if (ascending)
			comparator.add(
					getter(cast(arg(0), fieldClass), fieldName),
					getter(cast(arg(1), fieldClass), fieldName));
		else
			comparator.add(
					getter(cast(arg(1), fieldClass), fieldName),
					getter(cast(arg(0), fieldClass), fieldName));

		builder.method("compare", comparator);

		return builder.newInstance();
	}

	private static HttpResponse createResponse(String body) {
		return HttpResponse.create()
				.contentType(MediaType.HTML_UTF_8.toString())
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

	private static HttpResponse response500(String message) {
		HttpResponse response500 = HttpResponse.internalServerError500();
		response500.body(wrapUTF8(message));
		return response500;
	}

	private static HttpResponse response404(String message) {
		HttpResponse response404 = HttpResponse.notFound404();
		response404.body(wrapUTF8(message));
		return response404;
	}
}
