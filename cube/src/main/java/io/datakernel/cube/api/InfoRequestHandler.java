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

import com.google.common.collect.Sets;
import com.google.gson.*;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.AvailableDrillDowns;
import io.datakernel.cube.Cube;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.cube.api.CommonUtils.*;

public final class InfoRequestHandler implements AsyncHttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(InfoRequestHandler.class);

	private final Cube cube;
	private final ReportingConfiguration reportingConfiguration;
	private final Gson gson;
	private final DefiningClassLoader classLoader;
	private final Resolver resolver;

	public InfoRequestHandler(Cube cube, Gson gson, DefiningClassLoader classLoader) {
		this.cube = cube;
		this.reportingConfiguration = cube.getReportingConfiguration();
		this.gson = gson;
		this.classLoader = classLoader;
		this.resolver = new Resolver(classLoader, cube.getResolvers());
	}

	@Override
	public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
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

	private void processRequest(HttpRequest request, ResultCallback<HttpResponse> callback) {
		Stopwatch sw = Stopwatch.createStarted();
		Set<String> dimensions = getSetOfStrings(gson, request.getParameter("dimensions"));
		Set<String> measures = getSetOfStrings(gson, request.getParameter("measures"));
		String attributesJson = request.getParameter("attributes");
		Set<String> attributes = attributesJson == null ? Sets.<String>newHashSet() : getSetOfStrings(gson, attributesJson);
		String filtersJson = request.getParameter("filters");

		AggregationQuery.QueryPredicates queryPredicates = filtersJson == null ? new AggregationQuery.QueryPredicates() : gson.fromJson(filtersJson, AggregationQuery.QueryPredicates.class);
		Map<String, AggregationQuery.QueryPredicate> predicates = queryPredicates.asMap();

		Set<String> storedMeasures = newHashSet();
		for (String measure : measures) {
			if (reportingConfiguration.containsComputedMeasure(measure))
				storedMeasures.addAll(reportingConfiguration.getComputedMeasureDependencies(measure));
			else
				storedMeasures.add(measure);
		}

		AvailableDrillDowns availableDrillDowns = cube.getAvailableDrillDowns(dimensions, queryPredicates, storedMeasures);

		Map<AttributeResolver, List<String>> resolverKeys = newLinkedHashMap();
		Map<String, Class<?>> attributeTypes = newLinkedHashMap();
		Map<String, Object> keyConstants = newHashMap();

		for (String attribute : attributes) {
			AttributeResolver resolver = reportingConfiguration.getAttributeResolver(attribute);
			if (resolver == null)
				throw new QueryException("Cube does not contain resolver for '" + attribute + "'");

			List<String> key = reportingConfiguration.getKeyForResolver(resolver);

			boolean missedPrefix = false;
			for (String keyComponent : key) {
				if (predicates.get(keyComponent) instanceof AggregationQuery.QueryPredicateEq) {
					if (missedPrefix)
						throw new QueryException("Prefix of this compound key is not fully defined");
					else
						keyConstants.put(keyComponent, ((AggregationQuery.QueryPredicateEq) predicates.get(keyComponent)).value);
				} else {
					missedPrefix = true;
				}
			}

			resolverKeys.put(resolver, key);
			attributeTypes.put(attribute, reportingConfiguration.getAttributeType(attribute));
		}

		Class<?> attributesClass = createAttributesClass(attributeTypes);
		Object attributesObject = instantiate(attributesClass);

		resolver.resolve(Arrays.asList(attributesObject), attributesClass, attributeTypes, resolverKeys, keyConstants);

		String resultJson = constructJson(availableDrillDowns, attributesClass, attributesObject, attributeTypes.keySet(), measures);

		callback.onResult(createResponse(resultJson));

		logger.info("Sent response to GET /info request {} [time={}]", request, sw);
	}

	private String constructJson(AvailableDrillDowns availableDrillDowns, Class<?> attributesClass,
	                             Object attributesObject, Set<String> attributes, Set<String> requestedMeasures) {
		JsonObject jsonResult = new JsonObject();
		JsonArray jsonDrillDowns = new JsonArray();
		JsonArray jsonMeasures = new JsonArray();
		JsonObject jsonAttributes = new JsonObject();

		for (String measure : requestedMeasures) {
			if (availableDrillDowns.getMeasures().contains(measure)) {
				jsonMeasures.add(new JsonPrimitive(measure));
			} else if (reportingConfiguration.containsComputedMeasure(measure)) {
				Set<String> computedMeasureDependencies = reportingConfiguration.getComputedMeasureDependencies(measure);
				if (all(computedMeasureDependencies, in(availableDrillDowns.getMeasures())))
					jsonMeasures.add(new JsonPrimitive(measure));
			}
		}

		for (List<String> drillDown : availableDrillDowns.getDrillDowns()) {
			JsonArray jsonDrillDown = new JsonArray();
			for (String dimension : drillDown) {
				jsonDrillDown.add(new JsonPrimitive(dimension));
			}
			jsonDrillDowns.add(jsonDrillDown);
		}

		for (String attribute : attributes) {
			Object resolvedAttribute = generateGetter(classLoader, attributesClass, attribute).get(attributesObject);
			jsonAttributes.add(attribute, resolvedAttribute == null ? null : new JsonPrimitive(resolvedAttribute.toString()));
		}

		jsonResult.add("drillDowns", jsonDrillDowns);
		jsonResult.add("measures", jsonMeasures);
		jsonResult.add("attributes", jsonAttributes);

		return jsonResult.toString();
	}

	private Object instantiate(Class<?> attributesClass) {
		try {
			return attributesClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	private Class<?> createAttributesClass(Map<String, Class<?>> attributeTypes) {
		AsmBuilder<Object> builder = new AsmBuilder<>(classLoader, Object.class);
		for (Map.Entry<String, Class<?>> nameEntry : attributeTypes.entrySet()) {
			builder.field(nameEntry.getKey(), nameEntry.getValue());
		}
		return builder.defineClass();
	}
}
