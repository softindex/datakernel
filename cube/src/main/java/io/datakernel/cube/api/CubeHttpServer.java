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
import com.google.gson.GsonBuilder;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.async.ParseException;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncHttpServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.MiddlewareServlet;

public final class CubeHttpServer {
	public static final String QUERY_REQUEST_PATH = "/";

	public static MiddlewareServlet createServlet(Cube cube, Eventloop eventloop, DefiningClassLoader classLoader) {
		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.Predicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.registerTypeAdapter(CubeQuery.Ordering.class, new QueryOrderingGsonSerializer())
				.create();

		MiddlewareServlet servlet = new MiddlewareServlet();

		final HttpRequestHandler handler = new HttpRequestHandler(gson, cube, eventloop, classLoader);

		servlet.get(QUERY_REQUEST_PATH, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				handler.process(request, callback);
			}
		});

		servlet.get("/consolidation-debug", new ConsolidationDebugServlet(cube));

		return servlet;
	}

	public static AsyncHttpServer createServer(Cube cube, Eventloop eventloop, DefiningClassLoader classLoader) {
		return new AsyncHttpServer(eventloop, createServlet(cube, eventloop, classLoader));
	}

	public static AsyncHttpServer createServer(Cube cube, Eventloop eventloop, DefiningClassLoader classLoader, int port) {
		return createServer(cube, eventloop, classLoader).setListenPort(port);
	}
}
