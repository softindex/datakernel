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

import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.aggregation.QueryException;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.ICube;
import io.datakernel.cube.QueryResult;
import io.datakernel.cube.ReportType;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.util.Stopwatch;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.cube.http.Utils.*;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.toList;

public final class ReportingServiceServlet extends AsyncServletWithStats {
	private static final Logger logger = Logger.getLogger(ReportingServiceServlet.class.getName());

	private final ICube cube;
	private final CodecFactory mapping;
	private StructuredCodec<QueryResult> queryResultCodec;
	private StructuredCodec<AggregationPredicate> aggregationPredicateCodec;

	private ReportingServiceServlet(Eventloop eventloop, ICube cube, CodecFactory mapping) {
		super(eventloop);
		this.cube = cube;
		this.mapping = mapping;
	}

	public static ReportingServiceServlet create(Eventloop eventloop, ICube cube) {
		return new ReportingServiceServlet(eventloop, cube, CUBE_TYPES);
	}

	public static RoutingServlet createRootServlet(Eventloop eventloop, ICube cube) {
		return createRootServlet(
				ReportingServiceServlet.create(eventloop, cube));
	}

	public static RoutingServlet createRootServlet(ReportingServiceServlet reportingServiceServlet) {
		return RoutingServlet.create()
				.with(GET, "/", reportingServiceServlet);
	}

	private StructuredCodec<AggregationPredicate> getAggregationPredicateCodec() {
		if (aggregationPredicateCodec == null) {
			aggregationPredicateCodec = AggregationPredicateCodec.create(mapping, cube.getAttributeTypes(), cube.getMeasureTypes());
		}
		return aggregationPredicateCodec;
	}

	private StructuredCodec<QueryResult> getQueryResultCodec() {
		if (queryResultCodec == null) {
			queryResultCodec = QueryResultCodec.create(mapping, cube.getAttributeTypes(), cube.getMeasureTypes());
		}
		return queryResultCodec;
	}

	@NotNull
	@Override
	public Promise<HttpResponse> doServe(@NotNull HttpRequest httpRequest) {
		logger.log(INFO, () -> "Received request: " + httpRequest);
		try {
			Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			CubeQuery cubeQuery = parseQuery(httpRequest);
			return cube.query(cubeQuery)
					.map(queryResult -> {
						Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
						String json = toJson(getQueryResultCodec(), queryResult);
						HttpResponse httpResponse = createResponse(json);
						logger.log(INFO, "Processed request " + httpRequest + " (" + cubeQuery + ") " +
										"[totalTime=" + totalTimeStopwatch + ", jsonConstruction=" + resultProcessingStopwatch + "]");
						return httpResponse;
					});
		} catch (QueryException e) {
			logger.log(SEVERE,  e, () -> "Query exception: " + httpRequest);
			return Promise.of(createErrorResponse(e.getMessage()));
		} catch (ParseException e) {
			logger.log(SEVERE, e, () -> "Parse exception: " + httpRequest);
			return Promise.of(createErrorResponse(e.getMessage()));
		}
	}

	private static HttpResponse createResponse(String body) {
		HttpResponse response = HttpResponse.ok200();
		response.addHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.JSON, StandardCharsets.UTF_8)));
		response.setBody(wrapUtf8(body));
		response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	private static HttpResponse createErrorResponse(String body) {
		HttpResponse response = HttpResponse.ofCode(400);
		response.addHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT, StandardCharsets.UTF_8)));
		response.setBody(wrapUtf8(body));
		response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	private static Pattern splitter = Pattern.compile(",");

	private static List<String> split(String input) {
		return splitter.splitAsStream(input)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(toList());
	}

	public CubeQuery parseQuery(HttpRequest request) throws ParseException {
		CubeQuery query = CubeQuery.create();

		String parameter;
		parameter = request.getQueryParameter(ATTRIBUTES_PARAM);
		if (parameter != null)
			query.withAttributes(split(parameter));

		parameter = request.getQueryParameter(MEASURES_PARAM);
		if (parameter != null)
			query.withMeasures(split(parameter));

		parameter = request.getQueryParameter(WHERE_PARAM);
		if (parameter != null)
			query.withWhere(fromJson(getAggregationPredicateCodec(), parameter));

		parameter = request.getQueryParameter(SORT_PARAM);
		if (parameter != null)
			query.withOrderings(parseOrderings(parameter));

		parameter = request.getQueryParameter(HAVING_PARAM);
		if (parameter != null)
			query.withHaving(fromJson(getAggregationPredicateCodec(), parameter));

		parameter = request.getQueryParameter(LIMIT_PARAM);
		if (parameter != null)
			query.withLimit(Integer.valueOf(parameter));// TODO throws ParseException

		parameter = request.getQueryParameter(OFFSET_PARAM);
		if (parameter != null)
			query.withOffset(Integer.valueOf(parameter));

		parameter = request.getQueryParameter(REPORT_TYPE_PARAM);
		if (parameter != null)
			query.withReportType(ReportType.valueOf(parameter.toUpperCase()));

		return query;
	}

}
