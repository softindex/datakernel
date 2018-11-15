/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.cube.http;

import com.google.gson.TypeAdapter;
import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.aggregation.QueryException;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.cube.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.json.GsonAdapters.TypeAdapterMapping;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.cube.http.Utils.*;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static java.util.stream.Collectors.toList;

public final class ReportingServiceServlet extends AsyncServletWithStats {
	protected final Logger logger = LoggerFactory.getLogger(ReportingServiceServlet.class);

	private final ICube cube;
	private final TypeAdapterMapping mapping;
	private TypeAdapter<QueryResult> queryResultJson;
	private TypeAdapter<AggregationPredicate> aggregationPredicateJson;

	private ReportingServiceServlet(Eventloop eventloop, ICube cube, TypeAdapterMapping mapping) {
		super(eventloop);
		this.cube = cube;
		this.mapping = mapping;
	}

	public static ReportingServiceServlet create(Eventloop eventloop, ICube cube) {
		return new ReportingServiceServlet(eventloop, cube, CUBE_TYPES);
	}

	public static MiddlewareServlet createRootServlet(Eventloop eventloop, ICube cube) {
		return createRootServlet(
				ReportingServiceServlet.create(eventloop, cube),
				(cube instanceof Cube) ? ConsolidationDebugServlet.create(eventloop, (Cube) cube) : null);
	}

	public static MiddlewareServlet createRootServlet(ReportingServiceServlet reportingServiceServlet,
			@Nullable ConsolidationDebugServlet consolidationDebugServlet) {
		return MiddlewareServlet.create()
				.with(GET, "/", reportingServiceServlet)
				.with(GET, "/consolidation-debug", consolidationDebugServlet != null ?
						consolidationDebugServlet :
						new AsyncServlet() {
							@Override
							public Promise<HttpResponse> serve(HttpRequest request) {
								return Promise.of(HttpResponse.ofCode(404));
							}
						});
	}

	private TypeAdapter<AggregationPredicate> getAggregationPredicateJson() {
		if (aggregationPredicateJson == null) {
			aggregationPredicateJson = AggregationPredicateGsonAdapter.create(mapping, cube.getAttributeTypes(), cube.getMeasureTypes());
		}
		return aggregationPredicateJson;
	}

	private TypeAdapter<QueryResult> getQueryResultJson() {
		if (queryResultJson == null) {
			queryResultJson = QueryResultGsonAdapter.create(mapping, cube.getAttributeTypes(), cube.getMeasureTypes());
		}
		return queryResultJson;
	}

	@Override
	public Promise<HttpResponse> doServe(HttpRequest httpRequest) {
		logger.info("Received request: {}", httpRequest);
		try {
			Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			CubeQuery cubeQuery = parseQuery(httpRequest);
			return cube.query(cubeQuery).thenApply(queryResult -> {
				Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
				String json = getQueryResultJson().toJson(queryResult);
				HttpResponse httpResponse = createResponse(json);
				logger.info("Processed request {} ({}) [totalTime={}, jsonConstruction={}]", httpRequest,
						cubeQuery, totalTimeStopwatch, resultProcessingStopwatch);
				return httpResponse;
			});
		} catch (QueryException e) {
			logger.error("Query exception: " + httpRequest, e);
			return Promise.of(createErrorResponse(e.getMessage()));
		} catch (Exception e) {
			logger.error("Parse exception: " + httpRequest, e);
			return Promise.of(createErrorResponse(e.getMessage()));
		}
	}

	private static HttpResponse createResponse(String body) {
		HttpResponse response = HttpResponse.ok200();
		response.setHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.JSON, StandardCharsets.UTF_8)));
		response.setBody(wrapUtf8(body));
		response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	private static HttpResponse createErrorResponse(String body) {
		HttpResponse response = HttpResponse.ofCode(400);
		response.setHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT, StandardCharsets.UTF_8)));
		response.setBody(wrapUtf8(body));
		response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	private static Pattern splitter = Pattern.compile(",");

	private static List<String> split(String input) {
		return splitter.splitAsStream(input)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(toList());
	}

	@SuppressWarnings("unchecked")
	public CubeQuery parseQuery(HttpRequest request) throws Exception {
		CubeQuery query = CubeQuery.create();

		String parameter;
		parameter = request.getQueryParameterOrNull(ATTRIBUTES_PARAM);
		if (parameter != null)
			query.withAttributes(split(parameter));

		parameter = request.getQueryParameterOrNull(MEASURES_PARAM);
		if (parameter != null)
			query.withMeasures(split(parameter));

		parameter = request.getQueryParameterOrNull(WHERE_PARAM);
		if (parameter != null)
			query.withWhere(getAggregationPredicateJson().fromJson(parameter));

		parameter = request.getQueryParameterOrNull(SORT_PARAM);
		if (parameter != null)
			query.withOrderings(parseOrderings(parameter));

		parameter = request.getQueryParameterOrNull(HAVING_PARAM);
		if (parameter != null)
			query.withHaving(getAggregationPredicateJson().fromJson(parameter));

		parameter = request.getQueryParameterOrNull(LIMIT_PARAM);
		if (parameter != null)
			query.withLimit(Integer.valueOf(parameter));// TODO throws ParseException

		parameter = request.getQueryParameterOrNull(OFFSET_PARAM);
		if (parameter != null)
			query.withOffset(Integer.valueOf(parameter));

		parameter = request.getQueryParameterOrNull(REPORT_TYPE_PARAM);
		if (parameter != null)
			query.withReportType(ReportType.valueOf(parameter.toUpperCase()));

		return query;
	}

}
