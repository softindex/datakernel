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
import com.google.gson.JsonParseException;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.cube.api.CommonUtils.response400;
import static io.datakernel.cube.api.CommonUtils.response500;

public final class HttpRequestHandler implements RequestHandler<HttpRequest, HttpResponse> {
	private static final Logger logger = LoggerFactory.getLogger(HttpRequestHandler.class);

	private final HttpRequestProcessor httpRequestProcessor;
	private final RequestExecutor requestExecutor;
	private final HttpResultProcessor httpResultProcessor;

	public HttpRequestHandler(Gson gson, Cube cube, Eventloop eventloop, DefiningClassLoader classLoader) {
		this.httpRequestProcessor = new HttpRequestProcessor(gson);
		this.requestExecutor = new RequestExecutor(cube, cube.getStructure(), cube.getReportingConfiguration(),
				eventloop, classLoader, new Resolver(classLoader, cube.getResolvers()));
		this.httpResultProcessor = new HttpResultProcessor(classLoader, cube.getStructure());
	}

	@Override
	public void process(final HttpRequest httpRequest, final ResultCallback<HttpResponse> resultCallback) {
		try {
			final Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			final ReportingQuery reportingQuery = httpRequestProcessor.apply(httpRequest);
			requestExecutor.execute(reportingQuery, new ResultCallback<QueryResult>() {
				@Override
				public void onResult(QueryResult result) {
					try {
						Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
						HttpResponse httpResponse = httpResultProcessor.apply(result);
						logger.info("Processed request {} ({}) [totalTime={}, jsonConstruction={}]", httpRequest,
								reportingQuery, totalTimeStopwatch, resultProcessingStopwatch);
						resultCallback.onResult(httpResponse);
					} catch (Exception e) {
						logger.error("Exception occurred while processing results", e);
						resultCallback.onResult(response500("Unknown server error"));
					}
				}

				@Override
				public void onException(Exception exception) {
					logger.error("Executing query {} failed.", reportingQuery, exception);
					resultCallback.onResult(response500("Unknown server error"));
				}
			});
		} catch (QueryException e) {
			logger.info("Request {} could not be processed because of error: {}", httpRequest, e.getMessage());
			resultCallback.onResult(response400(e.getMessage()));
		} catch (JsonParseException e) {
			logger.info("Failed to parse JSON in request {}", httpRequest);
			resultCallback.onResult(response400("Failed to parse JSON request"));
		} catch (RuntimeException e) {
			logger.error("Unknown exception occurred while processing request {}", httpRequest, e);
			resultCallback.onResult(response500("Unknown server error"));
		}
	}
}
