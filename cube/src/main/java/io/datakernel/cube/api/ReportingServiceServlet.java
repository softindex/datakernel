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

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationPredicate;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.ICube;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.cube.api.CommonUtils.createGsonBuilder;
import static io.datakernel.cube.api.HttpJsonConstants.*;
import static io.datakernel.http.HttpMethod.GET;

public final class ReportingServiceServlet implements AsyncServlet {
	protected final Logger logger = LoggerFactory.getLogger(ReportingServiceServlet.class);

	private static final Type LIST_OF_STRINGS = new TypeToken<List<String>>() {}.getType();
	private static final Type ORDERINGS = new TypeToken<List<CubeQuery.Ordering>>() {}.getType();

	private final Eventloop eventloop;

	private final ICube cube;
	private final Gson gson;

	private ReportingServiceServlet(Eventloop eventloop, ICube cube, Gson gson) {
		this.eventloop = eventloop;
		this.gson = gson;
		this.cube = cube;
	}

	public static ReportingServiceServlet create(Eventloop eventloop, ICube cube) {
		Gson gson = createGsonBuilder(cube.getAttributeTypes(), cube.getMeasureTypes()).create();
		return new ReportingServiceServlet(eventloop, cube, gson);
	}

	public static MiddlewareServlet createRootServlet(Eventloop eventloop, ICube cube) {
		MiddlewareServlet middlewareServlet = MiddlewareServlet.create()
				.with(GET, "/", create(eventloop, cube));
		if (cube instanceof Cube) {
			middlewareServlet = middlewareServlet
					.with(GET, "/consolidation-debug", ConsolidationDebugServlet.create((Cube) cube));
		}
		return middlewareServlet;
	}

	@Override
	public void serve(final HttpRequest httpRequest, final ResultCallback<HttpResponse> callback) {
		logger.info("Received request: {}", httpRequest);
		try {
			final Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			final CubeQuery cubeQuery = parseQuery(httpRequest);
			cube.query(cubeQuery, new ForwardingResultCallback<QueryResult>(callback) {
				@Override
				protected void onResult(QueryResult result) {
					Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
					String json = gson.toJson(result);
					HttpResponse httpResponse = createResponse(json);
					logger.info("Processed request {} ({}) [totalTime={}, jsonConstruction={}]", httpRequest,
							cubeQuery, totalTimeStopwatch, resultProcessingStopwatch);
					callback.setResult(httpResponse);
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

	private static HttpResponse createResponse(String body) {
		HttpResponse response = HttpResponse.ok200();
		response.setContentType(ContentType.of(MediaTypes.JSON, Charsets.UTF_8));
		response.setBody(wrapUtf8(body));
		response.addHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	@SuppressWarnings("unchecked")
	public CubeQuery parseQuery(HttpRequest request) throws ParseException {
//		List<String> dimensions = parseListOfStrings(request.getParameter(DIMENSIONS_PARAM));
		//		String searchString = request.getParameter(SEARCH_PARAM);

//		if (dimensions.isEmpty() && attributes.isEmpty())
//			throw new ParseException("At least one dimension or attribute must be specified");

		CubeQuery query = CubeQuery.create();

		String parameter;
		parameter = request.getParameter(ATTRIBUTES_PARAM);
		if (parameter != null)
			query = query.withAttributes((List<String>) gson.fromJson(parameter, LIST_OF_STRINGS));

		parameter = request.getParameter(MEASURES_PARAM);
		if (parameter != null)
			query = query.withMeasures((List<String>) gson.fromJson(parameter, LIST_OF_STRINGS));

		parameter = request.getParameter(FILTERS_PARAM);
		if (parameter != null)
			query = query.withPredicate(gson.fromJson(parameter, AggregationPredicate.class));

		parameter = request.getParameter(SORT_PARAM);
		if (parameter != null)
			query = query.withOrderings((List<CubeQuery.Ordering>) gson.fromJson(parameter, ORDERINGS));

		parameter = request.getParameter(HAVING_PARAM);
		if (parameter != null)
			query = query.withHaving(gson.fromJson(parameter, AggregationPredicate.class));

		parameter = request.getParameter(LIMIT_PARAM);
		if (parameter != null)
			query = query.withLimit(Integer.valueOf(parameter));

		parameter = request.getParameter(OFFSET_PARAM);
		if (parameter != null)
			query = query.withOffset(Integer.valueOf(parameter));

		return query;
	}

}
