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
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.HttpServletError;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.HttpResponse.badRequest400;

public final class HttpRequestHandler implements RequestHandler {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final HttpRequestProcessor httpRequestProcessor;
	private final RequestExecutor requestExecutor;
	private final HttpResultProcessor httpResultProcessor;

	private HttpRequestHandler(Gson gson, Cube cube, Eventloop eventloop,
	                           LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache) {
		this.httpRequestProcessor = HttpRequestProcessor.create(gson);
		this.requestExecutor = RequestExecutor.create(cube, cube.getStructure(), cube.getReportingConfiguration(),
				eventloop, Resolver.create(cube.getResolvers()), classLoaderCache);
		this.httpResultProcessor = HttpResultProcessor.create(cube.getStructure(), cube.getReportingConfiguration());
	}

	public static HttpRequestHandler create(Gson gson, Cube cube, Eventloop eventloop,
	                                        LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache) {
		return new HttpRequestHandler(gson, cube, eventloop, classLoaderCache);
	}

	@Override
	public void process(final HttpRequest httpRequest, final AsyncHttpServlet.Callback resultCallback) throws ParseException {
		logger.info("Received request: {}", httpRequest);
		try {
			final Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			final ReportingQuery reportingQuery = httpRequestProcessor.apply(httpRequest);
			requestExecutor.execute(reportingQuery, new ResultCallback<QueryResult>() {
				@Override
				public void onResult(QueryResult result) {
					Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
					HttpResponse httpResponse = httpResultProcessor.apply(result);
					logger.info("Processed request {} ({}) [totalTime={}, jsonConstruction={}]", httpRequest,
							reportingQuery, totalTimeStopwatch, resultProcessingStopwatch);
					resultCallback.onResult(httpResponse);
				}

				@Override
				public void onException(Exception e) {
					logger.error("Executing query {} failed.", reportingQuery, e);
					resultCallback.onHttpError(new HttpServletError(500, e));
				}
			});
		} catch (QueryException e) {
			logger.info("Request {} could not be processed because of error", httpRequest, e);
			resultCallback.onResult(badRequest400().withBody(wrapUtf8(e.getMessage())));
		} catch (JsonParseException e) {
			logger.info("Failed to parse JSON in request {}", httpRequest, e);
			resultCallback.onResult(badRequest400().withBody(wrapUtf8("Failed to parse JSON")));
		}
	}
}
