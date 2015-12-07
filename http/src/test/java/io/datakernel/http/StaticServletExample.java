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
import java.net.URL;
import java.nio.file.Paths;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class StaticServletExample {
	private static final int SERVER_PORT = 5556;

	public static void main(String[] args) throws IOException {

		final URL resources = Paths.get("./test_data").toUri().toURL();

		NioEventloop eventloop = new NioEventloop();
		MiddlewareServlet main = new MiddlewareServlet();

		System.out.println(resources);

		main.setDefault(new StaticServlet(StaticServlet.SimpleResourceLoader.create(eventloop, newCachedThreadPool(), resources)));

		AsyncHttpServer staticFileServer = new AsyncHttpServer(eventloop, main);
		staticFileServer.setListenPort(SERVER_PORT);
		staticFileServer.listen();

		System.out.println("Check http://localhost:5556/hello.html in your browser");
		eventloop.run();
	}
}
