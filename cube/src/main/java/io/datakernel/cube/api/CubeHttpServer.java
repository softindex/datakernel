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

import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.MiddlewareServlet;

public final class CubeHttpServer {
	public static final String QUERY_REQUEST_PATH = "/";

	private CubeHttpServer() {}

	private static MiddlewareServlet createServlet(Cube cube, ReportingServiceServlet reportingServiceServlet) {
		MiddlewareServlet servlet = MiddlewareServlet.create();
		servlet.get(QUERY_REQUEST_PATH, reportingServiceServlet);
		servlet.get("/consolidation-debug", ConsolidationDebugServlet.create(cube));
		return servlet;
	}

	public static AsyncHttpServer createServer(Cube cube, Eventloop eventloop,
	                                           ReportingServiceServlet reportingServiceServlet, int port) {
		return AsyncHttpServer.create(eventloop, createServlet(cube, reportingServiceServlet)).withListenPort(port);
	}

	public static AsyncHttpServer createServer(Cube cube, Eventloop eventloop, int classLoaderCacheSize, int port) {
		return createServer(cube, eventloop, ReportingServiceServlet.create(eventloop, cube,
				LRUCache.<ClassLoaderCacheKey, DefiningClassLoader>create(classLoaderCacheSize)), port);
	}

	public static CubeHttpServer create() {return new CubeHttpServer();}
}
