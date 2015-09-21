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
import com.google.gson.reflect.TypeToken;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.*;
import io.datakernel.cube.dimensiontype.DimensionType;
import io.datakernel.cube.dimensiontype.DimensionTypeDate;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
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
	 * @param port      port to listen
	 * @return server instance (not started)
	 */
	public static AsyncHttpServer httpServer(final Cube cube, NioEventloop eventloop, int port) {
		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(CubeQuery.class, new CubeQueryGsonSerializer())
				.registerTypeAdapter(CubeQuery.CubePredicates.class, new CubePredicatesGsonSerializer(cube.getStructure()))
				.create();

		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.get(INFO_REQUEST_PATH, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				logger.info("Got request for available drill downs.");
				try {
					Set<String> dimensions = getStringsFromJsonArray(gson, request.getParameter("dimensions"));
					Set<String> measures = getStringsFromJsonArray(gson, request.getParameter("measures"));
					CubeQuery.CubePredicates cubePredicates = gson.fromJson(request.getParameter("filters"), CubeQuery.CubePredicates.class);
					AvailableDrillDowns availableDrillDowns =
							cube.getAvailableDrillDowns(dimensions, Sets.newHashSet(cubePredicates.predicates()), measures);

					String responseJson = gson.toJson(availableDrillDowns);

					callback.onResult(HttpResponse.create().body(wrapUTF8(responseJson)));
				} catch (Exception e) {
					callback.onResult(processException(e));
				}
			}
		});

		servlet.get(QUERY_REQUEST_PATH, queryHandler(gson, cube, eventloop, false));

		servlet.get(DIMENSIONS_REQUEST_PATH, queryHandler(gson, cube, eventloop, true));

		return new AsyncHttpServer(eventloop, servlet).setListenPort(port);
	}

	private static AsyncHttpServlet queryHandler(final Gson gson, final Cube cube, final NioEventloop eventloop,
	                                             final boolean ignoreMeasures) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
				try {
					final String queryJson = request.getParameter("query");
					logger.trace("Got query: {}", queryJson);
					final CubeQuery receivedQuery = gson.fromJson(queryJson, CubeQuery.class);
					Set<String> availableMeasures = cube.getAvailableMeasures(receivedQuery.getResultDimensions(), receivedQuery.getResultMeasures());

					final CubeQuery finalQuery = new CubeQuery()
							.dimensions(receivedQuery.getResultDimensions())
							.measures(newArrayList(availableMeasures))
							.predicates(receivedQuery.getPredicates());

					Class<?> resultClass = cube.getStructure().createResultClass(finalQuery);
					final StreamConsumers.ToList<?> consumerStream = queryCube(resultClass, finalQuery, cube, eventloop);

					consumerStream.addCompletionCallback(new CompletionCallback() {
						@Override
						public void onComplete() {
							String jsonResult = constructJson(gson, cube, consumerStream.getList(), finalQuery, ignoreMeasures);
							callback.onResult(HttpResponse.create().body(wrapUTF8(jsonResult)));
							logger.trace("Sending response {} to query {}.", jsonResult, finalQuery);
						}

						@Override
						public void onException(Exception e) {
							logger.error("Sending response to query {} failed.", finalQuery, e);
						}
					});
				} catch (Exception e) {
					callback.onResult(processException(e));
				}
			}
		};
	}

	private static <T> StreamConsumers.ToList<T> queryCube(Class<T> resultClass, CubeQuery query, Cube cube,
	                                                       NioEventloop eventloop) {
		StreamConsumers.ToList<T> consumerStream = StreamConsumers.toList(eventloop);
		cube.query(0, resultClass, query).streamTo(consumerStream);
		return consumerStream;
	}

	private static Set<String> getStringsFromJsonArray(Gson gson, String json) {
		Type type = new TypeToken<Set<String>>() {
		}.getType();
		return gson.fromJson(json, type);
	}

	private static <T> String constructJson(Gson gson, Cube cube, List<T> results, CubeQuery query, boolean ignoreMeasures) {
		List<String> resultDimensions = query.getResultDimensions();
		List<String> resultMeasures = query.getResultMeasures();
		JsonArray jsonResults = new JsonArray();
		CubeStructure structure = cube.getStructure();

		try {
			for (T result : results) {
				Class<?> resultClass = result.getClass();
				JsonObject resultJsonObject = new JsonObject();

				for (String dimension : resultDimensions) {
					DimensionType dimensionType = structure.getDimensionType(dimension);
					Field dimensionField = resultClass.getDeclaredField(dimension);
					dimensionField.setAccessible(true);
					Object fieldValue = dimensionField.get(result);
					if (dimensionType instanceof DimensionTypeDate) {
						String fieldValueString = dimensionType.toString(fieldValue);
						resultJsonObject.add(dimension, new JsonPrimitive(fieldValueString));
					} else {
						resultJsonObject.add(dimension, gson.toJsonTree(fieldValue));
					}
				}

				if (!ignoreMeasures) {
					for (String measure : resultMeasures) {
						Field measureField = resultClass.getDeclaredField(measure);
						measureField.setAccessible(true);
						Object measureValue = measureField.get(result);
						resultJsonObject.add(measure, gson.toJsonTree(measureValue));
					}
				}

				jsonResults.add(resultJsonObject);
			}
		} catch (Exception e) {
			logger.trace("Reflection exception thrown while trying to serialize fields. Query: {}. Cube: {}", query, e);
		}

		return jsonResults.toString();
	}

	private static HttpResponse processException(Exception exception) {
		HttpResponse internalServerError = HttpResponse.internalServerError500();
		if (exception instanceof CubeException) {
			internalServerError.body(wrapUTF8(exception.getMessage()));
		}
		return internalServerError;
	}
}
