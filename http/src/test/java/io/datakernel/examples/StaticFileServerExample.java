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

package io.datakernel.examples;

import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.middleware.MiddlewareServlet;

import java.io.IOException;

import static io.datakernel.http.middleware.MiddlewareStaticServer.serveStatic;

/* Example, which demonstrates the use of middleware, that serves static files.
To run this example, follow these steps:
1. (Optional) Change the port, to which HTTP server is bound.
2. (Optional) Adjust root URL for serving static files (URL constant).
3. (Optional) Change FILE_PATH to point to the directory with static files that are to be served.
4. Put some static files in directory, specified in FILE_PATH.
5. Run main() in $MODULE_DIR$.

For example, if you have specified "/static/" in URL and "./test_data" in FILE_PATH and have put
"hello.html" in "test_data", it will be served at http://<domain>/static/hello.html.
Similarly, if "styles.css" resides in "test_data/css", it is available at http://<domain>/static/css/styles.css.
 */
public class StaticFileServerExample {
	private static final int SERVER_PORT = 45555;
	private static final String URL = "/static/";
	private static final String FILE_PATH = "./test_data";

	public static void main(String[] args) throws IOException {
		MiddlewareServlet servlet = new MiddlewareServlet();

		NioEventloop eventloop = new NioEventloop();

		/* If the requested file is not found at the specified directory, next middleware will be called.
		If there are no more middlewares in chain, 404 will be sent.
		*/
		servlet.use(URL, serveStatic(FILE_PATH, eventloop));

		/* Middleware chaining allows you to look for requested file in different directories.
		You can add the following line:
		servlet.use(URL, serveStatic(ANOTHER_FILE_PATH, eventloop));
		After that, if requested file is not found in FILE_PATH, there will be an attempt to find it in ANOTHER_FILE_PATH.
		If it is not in ANOTHER_FILE_PATH, "Not Found" response code will be sent as before.
		 */

		AsyncHttpServer staticFileServer = new AsyncHttpServer(eventloop, servlet);

		staticFileServer.setListenPort(SERVER_PORT);

		staticFileServer.listen();

		eventloop.run();
	}
}
