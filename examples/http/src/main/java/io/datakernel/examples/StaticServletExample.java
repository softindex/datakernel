/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoaders;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class StaticServletExample {
	private static final Path RESOURCE_DIR = Paths.get("src/main/resources/static/site");

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		StaticServlet staticServlet = StaticServlet.create(eventloop, StaticLoaders.ofPath(newCachedThreadPool(), RESOURCE_DIR));

		AsyncHttpServer server = AsyncHttpServer.create(eventloop, staticServlet)
				.withListenPort(8080);

		server.listen();

		System.out.println("Server is running");
		System.out.println("You can connect from browser by visiting 'http://localhost:8080/'");

		eventloop.run();
	}
}
