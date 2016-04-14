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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.HttpRequestsGenerator.GeneratorOptions;
import io.datakernel.http.HttpThrottlingServer.ServerOptions;
import io.datakernel.util.Stopwatch;

public class HttpServerRequestGeneratorTest {
	private static final String SERVER_URL = "http://localhost:" + HttpThrottlingServer.SERVER_PORT;

	static boolean parseCommandLine(String[] args) {
		for (String arg : args) {
			switch (arg) {
				case "-?":
				case "-h":
					System.err.println(HttpServerRequestGeneratorTest.class.getSimpleName() + " [options for server and generator]\n" +
							"options for server and generator:\n");
					ServerOptions.usage();
					GeneratorOptions.usage();
					return false;
			}
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		if (!parseCommandLine(args))
			return;

		ServerOptions serverOptions = ServerOptions.parseCommandLine(args);
		HttpThrottlingServer.info(serverOptions);

		GeneratorOptions generatorOptions = GeneratorOptions.parseCommandLine(args);
		generatorOptions.setUrl(SERVER_URL);
		HttpRequestsGenerator.info(generatorOptions);

		final Eventloop eventloop = new Eventloop();

		final HttpThrottlingServer server = new HttpThrottlingServer(eventloop, serverOptions);
		server.start();

		final HttpRequestsGenerator requestsGenerator = new HttpRequestsGenerator(eventloop, generatorOptions, new CompletionCallback() {
			@Override
			protected void onComplete() {
				server.stop();
			}

			@Override
			protected void onException(Exception exception) {
				server.stop();
				System.err.println(exception.toString());
			}
		});
		requestsGenerator.start();
		Stopwatch stopwatch = Stopwatch.createStarted();
		eventloop.run();
		stopwatch.stop();
		requestsGenerator.printResults(stopwatch);
	}
}
