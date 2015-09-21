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

package io.datakernel.http;

import io.datakernel.eventloop.NioEventloop;

import java.io.IOException;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class StaticServletExample {
	private static final int SERVER_PORT = 5555;
	private static final String URL = "/static";
	private static final String FILE_PATH = "./test_data";

	public static void main(String[] args) throws IOException {

		MiddlewareServlet main = new MiddlewareServlet();

		NioEventloop eventloop = new NioEventloop();

		main.use(URL, new StaticServlet(FILE_PATH, eventloop, newCachedThreadPool()));

		AsyncHttpServer staticFileServer = new AsyncHttpServer(eventloop, main);

		staticFileServer.setListenPort(SERVER_PORT);

		staticFileServer.listen();

		eventloop.run();

		// Check localhost:5555/static/hello.html in your browser
	}
}
