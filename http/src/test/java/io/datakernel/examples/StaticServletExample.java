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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.StaticServletForFiles;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class StaticServletExample {
	private static final int SERVER_PORT = 5556;

	public static void main(String[] args) throws IOException {

		Path path = Paths.get("./test_data");

		Eventloop eventloop = new Eventloop();
		MiddlewareServlet main = new MiddlewareServlet();

		main.setDefault(StaticServletForFiles.create(eventloop, newCachedThreadPool(), path));

		AsyncHttpServer staticFileServer = new AsyncHttpServer(eventloop, main);
		staticFileServer.setListenPort(SERVER_PORT);
		staticFileServer.listen();

		System.out.println("Check http://localhost:5556/hello.html in your browser");
		eventloop.run();
	}
}
