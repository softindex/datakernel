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
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.MiddlewareServlet;

public final class CubeHttpServer {
	private static final String DIMENSIONS_REQUEST_PATH = "/dimensions/";
	private static final String QUERY_REQUEST_PATH = "/query/";
	private static final String INFO_REQUEST_PATH = "/info/";
	private static final String REPORTING_QUERY_REQUEST_PATH = "/";

	public static MiddlewareServlet createServlet(Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader) {
		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.create();

		MiddlewareServlet servlet = new MiddlewareServlet();

		servlet.get(INFO_REQUEST_PATH, new InfoRequestHandler(cube, gson, classLoader));

		servlet.get(QUERY_REQUEST_PATH, new QueryHandler(gson, cube, eventloop, classLoader));

		servlet.get(DIMENSIONS_REQUEST_PATH, new DimensionsRequestHandler(gson, cube, eventloop, classLoader));

		servlet.get(REPORTING_QUERY_REQUEST_PATH, new ReportingQueryHandler(gson, cube, eventloop, classLoader));

		return servlet;
	}

	public static AsyncHttpServer createServer(Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader) {
		return new AsyncHttpServer(eventloop, createServlet(cube, eventloop, classLoader));
	}

	public static AsyncHttpServer createServer(Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader, int port) {
		return createServer(cube, eventloop, classLoader).setListenPort(port);
	}
}
