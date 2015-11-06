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

import com.google.gson.*;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.api.QueryResultPlaceholder;
import io.datakernel.aggregation_db.api.ReportingDSLExpression;
import io.datakernel.aggregation_db.api.TotalsPlaceholder;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.ExpressionComparator;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.api.CommonUtils.*;

public final class ReportingQueryHandler implements AsyncHttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(ReportingQueryHandler.class);

	private final Gson gson;
	private final Cube cube;
	private final NioEventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final Resolver resolver;

	public ReportingQueryHandler(Gson gson, Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader,
	                             Resolver resolver) {
		this.gson = gson;
		this.cube = cube;
		this.eventloop = eventloop;
		this.classLoader = classLoader;
		this.resolver = resolver;
	}

	@Override
	public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
		try {
			processRequest(request, callback);
		} catch (QueryException e) {
			logger.info("Request {} could not be processed because of error: {}", request, e.getMessage());
			callback.onResult(response500(e.getMessage()));
		} catch (JsonParseException e) {
			logger.info("Failed to parse JSON in request {}", request);
			callback.onResult(response500("Failed to parse JSON request"));
		} catch (RuntimeException e) {
			logger.error("Unknown exception occurred while processing request {}", request, e);
			callback.onResult(response500("Unknown server error"));
		}
	}

	private void processRequest(HttpRequest request, final ResultCallback<HttpResponse> callback) {
		logger.info("Got query {}", request);
		final AggregationStructure structure = cube.getStructure();
		final ReportingConfiguration reportingConfiguration = cube.getReportingConfiguration();
		final Map<String, List<String>> nameKeys = newHashMap();
		final Map<String, Class<?>> nameTypes = newHashMap();

		Set<String> requestDimensions = getSetOfStrings(gson, request.getParameter("dimensions"));
		Set<String> dimensions = newHashSet();

		for (String dimension : requestDimensions) {
			if (structure.containsKey(dimension))
				dimensions.add(dimension);
			else if (reportingConfiguration.containsName(dimension)) {
				List<String> keys = reportingConfiguration.getNameKey(dimension);
				dimensions.addAll(keys);
				nameKeys.put(dimension, keys);
				nameTypes.put(dimension, reportingConfiguration.getNameType(dimension));
			} else
				throw new QueryException("Cube does not contain dimension with name '" + dimension + "'");
		}

		final List<String> queryMeasures = getListOfStrings(gson, request.getParameter("measures"));
		final Set<String> storedMeasures = newHashSet();
		final Set<String> computedMeasures = newHashSet();

		for (String queryMeasure : queryMeasures) {
			if (structure.containsOutputField(queryMeasure)) {
				storedMeasures.add(queryMeasure);
			} else if (reportingConfiguration.containsComputedMeasure(queryMeasure)) {
				ReportingDSLExpression reportingDSLExpression = reportingConfiguration.getExpressionForMeasure(queryMeasure);
				storedMeasures.addAll(reportingDSLExpression.getMeasureDependencies());
				computedMeasures.add(queryMeasure);
			} else {
				throw new QueryException("Cube does not contain measure with name '" + queryMeasure + "'");
			}
		}

		String predicatesJson = request.getParameter("filters");
		String orderingsJson = request.getParameter("sort");
		String limitString = request.getParameter("limit");
		String offsetString = request.getParameter("offset");

		final AggregationQuery finalQuery = new AggregationQuery()
				.keys(newArrayList(dimensions))
				.fields(newArrayList(storedMeasures));

		// parse ordering information
		List<String> ordering = getListOfStrings(gson, orderingsJson);
		boolean orderingByComputedMeasure = false;
		String orderingField = null;
		boolean ascendingOrdering = false;
		if (ordering != null) {
			if (ordering.size() != 2) {
				throw new QueryException("Incorrect 'sort' parameter format");
			}
			orderingField = ordering.get(0);
			orderingByComputedMeasure = computedMeasures.contains(orderingField);

			if (!dimensions.contains(orderingField) && !storedMeasures.contains(orderingField) && !orderingByComputedMeasure) {
				throw new QueryException("Ordering is specified by not requested field");
			}

			String direction = ordering.get(1);

			if (direction.equals("asc"))
				ascendingOrdering = true;
			else if (direction.equals("desc"))
				ascendingOrdering = false;
			else {
				throw new QueryException("Incorrect ordering specified in 'sort' parameter");
			}

			addOrderingToQuery(finalQuery, orderingField, ascendingOrdering, computedMeasures);
		}

		addPredicatesToQuery(finalQuery, predicatesJson);

		final Class<QueryResultPlaceholder> resultClass = createResultClass(classLoader, finalQuery, computedMeasures, nameTypes, structure, reportingConfiguration);
		final StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = queryCube(resultClass, finalQuery, cube, eventloop);

		final Integer limit = valueOrNull(limitString);
		final Integer offset = valueOrNull(offsetString);

		final boolean sortingRequired = orderingByComputedMeasure;
		final Comparator<QueryResultPlaceholder> comparator = sortingRequired ? generateComparator(classLoader, orderingField, ascendingOrdering, resultClass) : null;

		consumerStream.setResultCallback(new ResultCallback<List<QueryResultPlaceholder>>() {
			@Override
			public void onResult(List<QueryResultPlaceholder> results) {
				if (results.isEmpty()) {
					callback.onResult(createResponse(""));
					return;
				}

				// compute totals
				TotalsPlaceholder totalsPlaceholder = createTotalsPlaceholder(classLoader, structure, resultClass, reportingConfiguration, storedMeasures, computedMeasures);
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

				resolver.resolve((List) results, resultClass, nameKeys, nameTypes, new HashMap<String, Object>());

				// sort
				if (sortingRequired) {
					Collections.sort(results, comparator);
				}

				String jsonResult = constructQueryResultJson(resultClass, structure, queryMeasures,
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

	private static Integer valueOrNull(String str) {
		return str == null ? null : Integer.valueOf(str);
	}

	private void addPredicatesToQuery(AggregationQuery query, String predicatesJson) {
		AggregationQuery.QueryPredicates queryPredicates = null;

		if (predicatesJson != null) {
			queryPredicates = gson.fromJson(predicatesJson, AggregationQuery.QueryPredicates.class);
		}

		if (queryPredicates != null) {
			query.predicates(queryPredicates);
		}
	}

	private static void addOrderingToQuery(AggregationQuery query, String fieldName, boolean ascendingOrdering, Set<String> computedMeasures) {
		if (computedMeasures.contains(fieldName))
			return;

		if (ascendingOrdering)
			query.orderAsc(fieldName);
		else
			query.orderDesc(fieldName);
	}

	@SuppressWarnings("unchecked")
	private static StreamConsumers.ToList<QueryResultPlaceholder> queryCube(Class<QueryResultPlaceholder> resultClass,
	                                                                        AggregationQuery query,
	                                                                        Cube cube,
	                                                                        NioEventloop eventloop) {
		StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = StreamConsumers.toList(eventloop);
		StreamProducer<QueryResultPlaceholder> queryResultProducer = cube.query(resultClass, query);
		queryResultProducer.streamTo(consumerStream);
		return consumerStream;
	}

	private static Class<QueryResultPlaceholder> createResultClass(DefiningClassLoader classLoader, AggregationQuery query,
	                                                               Set<String> computedMeasureNames, Map<String, Class<?>> nameTypes,
	                                                               AggregationStructure structure, ReportingConfiguration reportingConfiguration) {
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
		for (Map.Entry<String, Class<?>> nameEntry : nameTypes.entrySet()) {
			builder.field(nameEntry.getKey(), nameEntry.getValue());
		}
		ExpressionSequence computeSequence = sequence();
		for (String computedMeasure : computedMeasureNames) {
			builder.field(computedMeasure, double.class);
			computeSequence.add(set(getter(self(), computedMeasure), reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
		}
		builder.method("computeMeasures", computeSequence);
		return builder.defineClass();
	}

	private static <T> String constructQueryResultJson(Class<?> resultClass, AggregationStructure structure, List<String> resultFields,
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
				resultJsonObject.add(resultKeys.get(j), keyTypes[j].toJson(keyGetters[j].get(result)));
			}

			for (int j = 0; j < resultFields.size(); j++) {
				resultJsonObject.add(resultFields.get(j), new JsonPrimitive((Number) fieldGetters[j].get(result)));
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

	private static TotalsPlaceholder createTotalsPlaceholder(DefiningClassLoader classLoader, AggregationStructure structure,
	                                                         Class<?> inputClass, ReportingConfiguration reportingConfiguration,
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
			computeSequence.add(set(getter(self(), computedMeasure), reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
		}
		builder.method("computeMeasures", computeSequence);

		return builder.newInstance();
	}

	private static Comparator<QueryResultPlaceholder> generateComparator(DefiningClassLoader classLoader, String fieldName, boolean ascending,
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
}
